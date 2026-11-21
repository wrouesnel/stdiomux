package yamux

import (
	"bytes"
	"context"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// shapedLink is one direction of a connection which delivers what's written to it after
// a delay, at a limited rate, buffering without limit, as a network does.
type shapedLink struct {
	delay time.Duration
	rate  float64 // bytes a second

	mu sync.Mutex
	// free is when the link has sent everything written so far.
	free   time.Time
	closed bool
	chunks chan shapedChunk
	r      *io.PipeReader
}

type shapedChunk struct {
	data []byte
	at   time.Time
}

func newShapedLink(delay time.Duration, rate float64) *shapedLink {
	r, w := io.Pipe()
	l := &shapedLink{delay: delay, rate: rate, chunks: make(chan shapedChunk, 1<<16), r: r}
	go func() {
		for c := range l.chunks {
			time.Sleep(time.Until(c.at))
			if _, err := w.Write(c.data); err != nil {
				return
			}
		}
		_ = w.Close()
	}()
	return l
}

func (l *shapedLink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, io.ErrClosedPipe
	}
	sent := time.Now()
	if l.free.After(sent) {
		sent = l.free
	}
	sent = sent.Add(time.Duration(float64(len(p)) / l.rate * float64(time.Second)))
	l.free = sent
	l.chunks <- shapedChunk{data: bytes.Clone(p), at: sent.Add(l.delay)}
	return len(p), nil
}

// shapedConn is one end of a connection over two shaped links.
type shapedConn struct {
	io.Reader
	out *shapedLink
}

func (c shapedConn) Write(p []byte) (int, error) { return c.out.Write(p) }
func (c shapedConn) Close() error {
	c.out.mu.Lock()
	defer c.out.mu.Unlock()
	if !c.out.closed {
		c.out.closed = true
		close(c.out.chunks)
	}
	return nil
}
func (c shapedConn) LocalAddr() net.Addr              { return nil }
func (c shapedConn) RemoteAddr() net.Addr             { return nil }
func (c shapedConn) SetDeadline(time.Time) error      { return nil }
func (c shapedConn) SetReadDeadline(time.Time) error  { return nil }
func (c shapedConn) SetWriteDeadline(time.Time) error { return nil }

func shapedConns(delay time.Duration, rate float64) (net.Conn, net.Conn) {
	ab, ba := newShapedLink(delay, rate), newShapedLink(delay, rate)
	return shapedConn{ba.r, ab}, shapedConn{ab.r, ba}
}

type zeroes struct{}

func (zeroes) Read(p []byte) (int, error) { clear(p); return len(p), nil }

// TestEchoDuringDownload checks that a stream's round trip doesn't wait behind another
// stream's download. Stock yamux takes about 200 ms on this link.
func TestEchoDuringDownload(t *testing.T) {
	if testing.Short() {
		t.Skip("takes a few seconds")
	}
	median := echoDuringDownload(t)
	require.Less(t, median, 80*time.Millisecond)
}

// echoDuringDownload returns the median round trip of a stream on a 10 Mbit link,
// while another stream downloads.
func echoDuringDownload(t *testing.T) time.Duration {
	conn1, conn2 := shapedConns(time.Millisecond, 10e6/8)
	cfg := testConf()
	cfg.MaxStreamWindowSize = 16 << 20
	client, err := Client(conn1, cfg, nil)
	require.NoError(t, err)
	defer client.Close()
	server, err := Server(conn2, cfg, nil)
	require.NoError(t, err)
	defer server.Close()

	go func() {
		d, err := server.AcceptStream()
		if err != nil {
			return
		}
		go func() { _, _ = io.Copy(d, zeroes{}) }()
		e, err := server.AcceptStream()
		if err != nil {
			return
		}
		_, _ = io.Copy(e, e)
	}()
	download, err := client.OpenStream(context.Background())
	require.NoError(t, err)
	go func() { _, _ = io.Copy(io.Discard, download) }()
	_, err = download.Write([]byte("go"))
	require.NoError(t, err)
	echo, err := client.OpenStream(context.Background())
	require.NoError(t, err)
	time.Sleep(time.Second)

	buf := make([]byte, 1)
	var times []time.Duration
	for range 20 {
		start := time.Now()
		_, err := echo.Write([]byte("x"))
		require.NoError(t, err)
		_, err = io.ReadFull(echo, buf)
		require.NoError(t, err)
		times = append(times, time.Since(start))
	}
	slices.Sort(times)
	t.Logf("echo: median %v, worst %v", times[len(times)/2], times[len(times)-1])
	return times[len(times)/2]
}
