package yamux

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// feed has the peer receive rate bytes a second for d, with probes answered every
// probeInterval after rtt, starting at start. It returns the time it ends.
func feed(p *pacer, start time.Time, d time.Duration, rate float64, rtt time.Duration) time.Time {
	now := start
	for end := start.Add(d); now.Before(end); now = now.Add(probeInterval) {
		p.sent(now, 1)
		p.written.Add(int64(rate * probeInterval.Seconds()))
		p.probe(now)
		p.answered(now.Add(rtt))
	}
	return now
}

func TestPacerUnpacedWithoutQueue(t *testing.T) {
	var p pacer
	start := time.Unix(1000, 0)
	feed(&p, start, 2*time.Second, 10e6, time.Millisecond)
	require.Zero(t, p.rate, "no queue, no pacing")
	require.Zero(t, p.delay(start.Add(3*time.Second)))
}

func TestPacerPacesWhenQueueBuilds(t *testing.T) {
	var p pacer
	start := time.Unix(1000, 0)
	now := feed(&p, start, time.Second, 10e6, time.Millisecond)
	feed(&p, now, 100*time.Millisecond, 10e6, 20*time.Millisecond)
	require.InDelta(t, drainGain*10e6, p.rate, 1e6, "paced below the delivery rate to drain the queue")
}

func TestPacerEasesOffWhenQueueDrains(t *testing.T) {
	var p pacer
	start := time.Unix(1000, 0)
	now := feed(&p, start, time.Second, 10e6, time.Millisecond)
	now = feed(&p, now, 100*time.Millisecond, 10e6, 20*time.Millisecond)
	require.NotZero(t, p.rate)
	feed(&p, now, 2*time.Second, 10e6, time.Millisecond)
	require.Zero(t, p.rate, "pacing stops once the rate is well above delivery")
}

func TestPacerDelay(t *testing.T) {
	p := pacer{rate: 1e6}
	now := time.Unix(1000, 0)
	p.sent(now, 10000)
	require.Equal(t, 10*time.Millisecond, p.delay(now))
	require.Equal(t, 5*time.Millisecond, p.delay(now.Add(5*time.Millisecond)))
	// Time not used isn't banked.
	later := now.Add(time.Second)
	p.sent(later, 10000)
	require.Equal(t, 10*time.Millisecond, p.delay(later))
}

func TestPacerFrameLimit(t *testing.T) {
	var p pacer
	require.Equal(t, uint32(startFrame), p.frameLimit(), "before measuring")
	start := time.Unix(1000, 0)
	feed(&p, start, time.Second, 1.25e6, time.Millisecond)
	require.InDelta(t, 1.25e6*frameTime.Seconds(), float64(p.frameLimit()), 1000)

	var slow pacer
	feed(&slow, start, time.Second, 10e3, time.Millisecond)
	require.Equal(t, uint32(minFrame), slow.frameLimit())
}

func TestPacerProbes(t *testing.T) {
	var p pacer
	now := time.Unix(1000, 0)
	require.False(t, p.probeDue(now), "no data, no probes")
	p.sent(now, 1)
	require.True(t, p.probeDue(now))
	id := p.probe(now)
	require.NotZero(t, id&probePing)
	require.False(t, p.probeDue(now.Add(time.Hour)), "one probe at a time")
	p.answered(now.Add(time.Millisecond))
	require.False(t, p.probeDue(now.Add(10*time.Millisecond)))
	require.True(t, p.probeDue(now.Add(probeInterval)))
	require.False(t, p.probeDue(now.Add(2*time.Second)), "data stopped")
}
