package stdiomux

import (
	"io"
	"net"
)

// Pipe copies between two connections until both have finished sending, then closes
// them. When one finishes first, the other is half-closed, as TCP does, so it can still
// answer, where it can be: TCP connections and yamux streams can. Where it can't, both
// are closed then. If a copy fails, as when one is cut, both are reset, so their far ends
// stop too.
func Pipe(a, b io.ReadWriteCloser) {
	const (
		halfClosed = iota
		finished
		failed
	)
	done := make(chan int, 2)
	cp := func(dst io.ReadWriteCloser, src io.Reader) {
		_, err := io.Copy(dst, src)
		switch {
		case err != nil:
			done <- failed
		case CloseWrite(dst):
			done <- halfClosed
		default:
			done <- finished
		}
	}
	go cp(a, b)
	go cp(b, a)
	first := <-done
	switch first {
	case failed:
		Reset(a)
		Reset(b)
	case finished:
		_ = a.Close()
		_ = b.Close()
	}
	second := <-done
	switch {
	case first != halfClosed:
	case second == failed:
		Reset(a)
		Reset(b)
	default:
		_ = a.Close()
		_ = b.Close()
	}
}

// CloseWrite half-closes c, if it can be, reporting whether it was.
func CloseWrite(c any) bool {
	cw, ok := c.(interface{ CloseWrite() error })
	return ok && cw.CloseWrite() == nil
}

// Reset closes c at once, telling its far end it was cut short where it can: a yamux
// stream is reset, and a TCP connection sends RST.
func Reset(c io.Closer) {
	switch r := c.(type) {
	case interface{ Reset() error }:
		_ = r.Reset()
	case *net.TCPConn:
		_ = r.SetLinger(0)
	}
	_ = c.Close()
}
