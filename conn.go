// Package stdiomux runs many streams over one byte stream, such as the standard input and
// output of a program at the far end of an ssh connection. It uses the yamux in this
// module, which keeps an interactive stream's latency low while others transfer in bulk.
//
// Each stream carries frames: a type byte, a length and a payload. A stream usually begins
// with a frame saying what it's for, and the other end answers with one saying whether it
// can do it, but the meaning of frames is the caller's.
package stdiomux

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/wrouesnel/stdiomux/yamux"
)

// KeepAlive is how often each end checks the other is alive, by default.
const KeepAlive = 15 * time.Second

// DefaultConfig is the yamux configuration for a link over pipes. Each stream's window
// starts at 256 KiB and grows, while it's read as fast as it arrives, up to what fills a
// long, fast link: 16 MiB is 1 Gbit/s at 130 ms. Nothing is logged.
func DefaultConfig() *yamux.Config {
	c := yamux.DefaultConfig()
	c.KeepAliveInterval = KeepAlive
	c.ConnectionWriteTimeout = 30 * time.Second
	c.MaxStreamWindowSize = 16 << 20
	c.MaxIncomingStreams = 1 << 16
	c.LogOutput = io.Discard
	return c
}

// Client starts the end of a link which opens streams, over conn. A nil config is
// DefaultConfig's.
func Client(conn net.Conn, config *yamux.Config) (*yamux.Session, error) {
	if config == nil {
		config = DefaultConfig()
	}
	return yamux.Client(conn, config, nil)
}

// Server starts the end of a link which accepts streams, over conn. A nil config is
// DefaultConfig's.
func Server(conn net.Conn, config *yamux.Config) (*yamux.Session, error) {
	if config == nil {
		config = DefaultConfig()
	}
	return yamux.Server(conn, config, nil)
}

// Conn is a net.Conn over a reader and a writer, such as a program's standard output and
// input, for yamux to run over. Deadlines are ignored: pipes have none, and the keep-alive
// notices a link that has gone quiet.
type Conn struct {
	r       io.Reader
	w       io.Writer
	closers []io.Closer

	writeMu sync.Mutex
	once    sync.Once
	closed  chan struct{}
}

// NewConn makes a Conn which reads r and writes w. Closing it closes the closers.
func NewConn(r io.Reader, w io.Writer, closers ...io.Closer) *Conn {
	return &Conn{r: r, w: w, closers: closers, closed: make(chan struct{})}
}

func (c *Conn) Read(p []byte) (int, error) { return c.r.Read(p) }

func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	return c.w.Write(p)
}

// Close closes the closers, once, but only between writes: yamux closes its connection
// while its sender may still be writing, which some pipes, such as an ssh channel's, can't
// take. It doesn't wait for a write that's stuck: closing the closers ends that.
func (c *Conn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		go func() {
			c.writeMu.Lock()
			defer c.writeMu.Unlock()
			for _, cl := range c.closers {
				_ = cl.Close()
			}
		}()
	})
	return nil
}

// Done is closed once the Conn is closed.
func (c *Conn) Done() <-chan struct{} { return c.closed }

func (*Conn) LocalAddr() net.Addr              { return pipeAddr{} }
func (*Conn) RemoteAddr() net.Addr             { return pipeAddr{} }
func (*Conn) SetDeadline(time.Time) error      { return nil }
func (*Conn) SetReadDeadline(time.Time) error  { return nil }
func (*Conn) SetWriteDeadline(time.Time) error { return nil }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }
