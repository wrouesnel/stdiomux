package stdiomux

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrames(t *testing.T) {
	var buf bytes.Buffer
	w := NewFrameWriter(&buf)
	require.NoError(t, w.Write('a', []byte("hello")))
	require.NoError(t, w.Write('b', nil))
	require.NoError(t, w.WriteJSON('j', map[string]int{"x": 1}))
	r := NewFrameReader(&buf)
	typ, p, err := r.Next()
	require.NoError(t, err)
	require.Equal(t, byte('a'), typ)
	require.Equal(t, "hello", string(p))
	typ, p, err = r.Next()
	require.NoError(t, err)
	require.Equal(t, byte('b'), typ)
	require.Empty(t, p)
	var v map[string]int
	require.NoError(t, r.NextJSON('j', &v))
	require.Equal(t, 1, v["x"])
	_, _, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestFrameErrors(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, NewFrameWriter(&buf).Write('a', []byte("hello")))
	cut := buf.Bytes()[:buf.Len()-1]
	_, _, err := NewFrameReader(bytes.NewReader(cut)).Next()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	r := NewFrameReader(bytes.NewReader(buf.Bytes()))
	r.Max = 4
	_, _, err = r.Next()
	require.ErrorIs(t, err, ErrFrameTooLarge)

	buf.Reset()
	require.NoError(t, NewFrameWriter(&buf).Write('a', []byte("{}")))
	require.Error(t, NewFrameReader(&buf).NextJSON('b', &struct{}{}))
}

func TestFramesDontAllocate(t *testing.T) {
	var buf bytes.Buffer
	w := NewFrameWriter(&buf)
	r := NewFrameReader(&buf)
	payload := make([]byte, 1000)
	allocs := testing.AllocsPerRun(100, func() {
		_ = w.Write('a', payload)
		_, _, _ = r.Next()
	})
	require.Zero(t, allocs)
}

// link runs a client and server session over pipes.
func link(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	ar, bw := io.Pipe()
	br, aw := io.Pipe()
	return NewConn(ar, aw, ar, aw), NewConn(br, bw, br, bw)
}

func TestSessionOverPipes(t *testing.T) {
	a, b := link(t)
	client, err := Client(a, nil)
	require.NoError(t, err)
	defer client.Close()
	server, err := Server(b, nil)
	require.NoError(t, err)
	defer server.Close()

	go func() {
		for {
			s, err := server.AcceptStream()
			if err != nil {
				return
			}
			go func() {
				r, w := NewFrameReader(s), NewFrameWriter(s)
				for {
					typ, p, err := r.Next()
					if err != nil {
						_ = s.Close()
						return
					}
					_ = w.Write(typ+1, p)
				}
			}()
		}
	}()
	s, err := client.OpenStream(context.Background())
	require.NoError(t, err)
	r, w := NewFrameReader(s), NewFrameWriter(s)
	for i := range 3 {
		require.NoError(t, w.Write(byte(i), []byte("ping")))
		typ, p, err := r.Next()
		require.NoError(t, err)
		require.Equal(t, byte(i+1), typ)
		require.Equal(t, "ping", string(p))
	}
	require.NoError(t, s.CloseWrite())
	_, _, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestConnClose(t *testing.T) {
	a, b := link(t)
	client, err := Client(a, nil)
	require.NoError(t, err)
	server, err := Server(b, nil)
	require.NoError(t, err)
	defer server.Close()
	require.NoError(t, client.Close())
	select {
	case <-a.Done():
	case <-time.After(time.Second):
		t.Fatal("conn not closed")
	}
	select {
	case <-server.CloseChan():
	case <-time.After(5 * time.Second):
		t.Fatal("server didn't notice the link closing")
	}
	_, err = a.Write([]byte("x"))
	require.ErrorIs(t, err, net.ErrClosed)
}

func TestPipeHalfCloses(t *testing.T) {
	a, b := link(t)
	client, err := Client(a, nil)
	require.NoError(t, err)
	defer client.Close()
	server, err := Server(b, nil)
	require.NoError(t, err)
	defer server.Close()

	// The server pipes its stream to a second stream it opens back to the client.
	go func() {
		s, err := server.AcceptStream()
		if err != nil {
			return
		}
		back, err := server.OpenStream(context.Background())
		if err != nil {
			return
		}
		Pipe(s, back)
	}()
	s, err := client.OpenStream(context.Background())
	require.NoError(t, err)
	_, err = s.Write([]byte("hello"))
	require.NoError(t, err)
	back, err := client.AcceptStream()
	require.NoError(t, err)
	require.NoError(t, s.CloseWrite())
	got, err := io.ReadAll(back)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
	// The far end can still answer after the half-close.
	_, err = back.Write([]byte("reply"))
	require.NoError(t, err)
	require.NoError(t, back.CloseWrite())
	got, err = io.ReadAll(s)
	require.NoError(t, err)
	require.Equal(t, "reply", string(got))
}
