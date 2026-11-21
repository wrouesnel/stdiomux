package yamux

import (
	"sync"
	"sync/atomic"
	"time"
)

// Pacing keeps the data waiting to be sent in the session, where streams take turns,
// rather than in the connection beneath it, where a stream's frame waits behind all
// the data before it. It sends at about the rate the peer receives, and backs off when
// pings show a queue building in the connection.
//
// The pings measure the delivery rate too: when one's reply arrives, everything written
// before it has been delivered. Window updates would be a coarser measure: they come in
// steps of up to half the stream's window, which can be a large part of a second's data.

const (
	probeInterval = 20 * time.Millisecond
	// queueTarget is the queueing delay allowed in the connection.
	queueTarget = 10 * time.Millisecond
	rateSpan    = 250 * time.Millisecond
	minRate     = 32 << 10
	probePing   = 1 << 31
	// drainGain is the share of the delivery rate sent while a queue drains, and
	// probeGain how much faster than that each probe without a queue lets it send.
	drainGain = 0.9
	probeGain = 1.05
	// frameTime is how long a data frame should take to deliver, at the measured rate:
	// another stream's frame may have to wait that long behind it.
	frameTime = 5 * time.Millisecond
	// minFrame and startFrame bound data frames when the rate is slow, and before it's
	// been measured.
	minFrame   = 4 << 10
	startFrame = 16 << 10
)

type deliverySample struct {
	at        time.Time
	delivered int64
}

type pacer struct {
	mu sync.Mutex
	// written counts the bytes written to the connection.
	written atomic.Int64
	// frame is the largest data frame to send, from the delivery rate, or 0 before it's
	// been measured.
	frame atomic.Uint32

	rate     float64 // bytes a second; 0 is unpaced
	next     time.Time
	base     time.Duration
	samples  [32]deliverySample
	n        int
	rates    [10]float64
	ri       int
	lastData time.Time
	probeID  uint32
	probeAt  time.Time
	// probeWritten is written when the outstanding probe was sent.
	probeWritten int64
	waiting      bool
	draining     bool
}

// delay is how long until a data frame may be sent.
func (p *pacer) delay(now time.Time) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rate == 0 {
		return 0
	}
	return p.next.Sub(now)
}

func (p *pacer) sent(now time.Time, n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastData = now
	if p.rate == 0 {
		return
	}
	if p.next.Before(now) {
		p.next = now
	}
	p.next = p.next.Add(time.Duration(float64(n) / p.rate * float64(time.Second)))
}

// probeDue reports whether a ping should be sent to measure the queue.
func (p *pacer) probeDue(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.waiting && now.Sub(p.lastData) < time.Second && now.Sub(p.probeAt) >= probeInterval
}

func (p *pacer) probe(now time.Time) uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waiting = true
	p.probeAt = now
	p.probeWritten = p.written.Load()
	p.probeID++
	return probePing | p.probeID&^probePing
}

// answered takes a probe's reply.
func (p *pacer) answered(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waiting = false
	rtt := now.Sub(p.probeAt)
	if p.base == 0 || rtt < p.base {
		p.base = rtt
	}
	delivered := p.probeWritten
	s := deliverySample{now, delivered}
	p.samples[p.n%len(p.samples)] = s
	p.n++
	// The delivery rate over the last rateSpan.
	var old deliverySample
	for i := 1; i < p.n && i < len(p.samples); i++ {
		c := p.samples[(p.n-1-i)%len(p.samples)]
		old = c
		if now.Sub(c.at) >= rateSpan {
			break
		}
	}
	if old.at.IsZero() || now.Sub(old.at) < rateSpan/2 {
		return
	}
	dr := float64(delivered-old.delivered) / now.Sub(old.at).Seconds()
	p.rates[p.ri%len(p.rates)] = dr
	p.ri++
	best := 0.0
	for _, r := range p.rates {
		best = max(best, r)
	}
	if best == 0 {
		return
	}
	p.frame.Store(uint32(max(best*frameTime.Seconds(), minFrame)))
	// Pacing starts only once a queue builds: a stream that isn't sending as fast as
	// it can would otherwise hold itself to its own rate.
	switch {
	case rtt-p.base > queueTarget:
		// Drain the queue.
		p.rate = max(best*drainGain, minRate)
		p.draining = true
	case p.draining:
		// Drained: send at the rate the peer receives.
		p.rate = max(best, minRate)
		p.draining = false
	case p.rate > 0:
		// Look for more bandwidth, and stop pacing if there's plenty.
		p.rate *= probeGain
		if p.rate > 4*best {
			p.rate = 0
		}
	}
}

// active reports whether data has been sent lately, so the queue is being measured.
func (p *pacer) active(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.waiting && now.Sub(p.lastData) < time.Second
}

// frameLimit is the largest data frame to send now.
func (p *pacer) frameLimit() uint32 {
	if f := p.frame.Load(); f != 0 {
		return f
	}
	return startFrame
}
