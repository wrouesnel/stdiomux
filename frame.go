package stdiomux

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// FrameHeaderSize is the size of a frame's header: its type, then its payload's length as
// a big-endian uint32.
const FrameHeaderSize = 5

// MaxFrame is the largest payload a FrameReader takes, by default.
const MaxFrame = 16 << 20

// ErrFrameTooLarge is a frame longer than a FrameReader takes.
var ErrFrameTooLarge = errors.New("frame too large")

// FrameWriter writes frames to a stream. One frame is written at a time, in one write,
// so goroutines can share it.
type FrameWriter struct {
	w   io.Writer
	mu  sync.Mutex
	buf []byte
}

// NewFrameWriter writes frames to w.
func NewFrameWriter(w io.Writer) *FrameWriter { return &FrameWriter{w: w} }

// Write writes a frame of type typ.
func (f *FrameWriter) Write(typ byte, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buf = appendFrame(f.buf[:0], typ, payload)
	_, err := f.w.Write(f.buf)
	if cap(f.buf) > 64<<10 {
		// Don't keep a large frame's buffer for small ones.
		f.buf = nil
	}
	return err
}

// WriteJSON writes a frame of type typ holding v as JSON.
func (f *FrameWriter) WriteJSON(typ byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encoding frame: %w", err)
	}
	return f.Write(typ, b)
}

func appendFrame(dst []byte, typ byte, payload []byte) []byte {
	dst = append(dst, typ)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(payload))) //nolint:gosec // frames are small
	return append(dst, payload...)
}

// FrameReader reads frames from a stream.
type FrameReader struct {
	r io.Reader
	// Max is the largest payload taken: MaxFrame unless changed.
	Max int
	hdr [FrameHeaderSize]byte
	buf []byte
}

// NewFrameReader reads frames from r.
func NewFrameReader(r io.Reader) *FrameReader { return &FrameReader{r: r, Max: MaxFrame} }

// Next reads the next frame. Its payload is valid until the next call. At the end of the
// stream it returns io.EOF, or io.ErrUnexpectedEOF if a frame was cut short.
func (f *FrameReader) Next() (byte, []byte, error) {
	if _, err := io.ReadFull(f.r, f.hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(f.hdr[1:])
	if int64(n) > int64(f.Max) {
		return 0, nil, ErrFrameTooLarge
	}
	if cap(f.buf) < int(n) {
		f.buf = make([]byte, n)
	}
	f.buf = f.buf[:n]
	if _, err := io.ReadFull(f.r, f.buf); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return f.hdr[0], f.buf, nil
}

// NextJSON reads the next frame, which must be of type typ, into v as JSON.
func (f *FrameReader) NextJSON(typ byte, v any) error {
	got, payload, err := f.Next()
	if err != nil {
		return err
	}
	if got != typ {
		return fmt.Errorf("expected a frame of type %d, got %d", typ, got)
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("decoding frame: %w", err)
	}
	return nil
}
