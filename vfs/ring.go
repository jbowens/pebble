// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

type WriteContext struct {
	ring     IORing
	category DiskWriteCategory
}

// Writer wraps a file with a RingWriter that accepts sequential writes to the
// resulting file.
func (c *WriteContext) Create(f File) *RingWriter {
	return &RingWriter{
		wctx: c,
		f:    f,
		fd:   f.Fd(),
	}
}

// Buffer returns a buffer with the given capacity.
func (c *WriteContext) Buffer(n int) Buffer {
	return c.ring.Buffer(n)
}

func (c *WriteContext) Sync() error {
	return c.ring.Sync()
}

// Close closes the write context and releases resources. All writers created
// from the write context must have already been closed.
func (c *WriteContext) Close() error {
	return c.ring.Close()
}

type RingWriter struct {
	wctx *WriteContext
	f    File
	fd   uintptr
	err  error
}

func (w *RingWriter) Write(p []byte) error {
	_, w.err = w.wctx.ring.Submit(RingOp{
		Type: RingOpAppend,
		Fd:   w.fd,
		Data: p,
	})
	return w.err
}

func (w *RingWriter) Finish() error {
	return nil
}

func (w *RingWriter) Abort() {}

type Buffer struct {
	b []byte
}

type IORing interface {
	Buffer(n int) Buffer
	Sync() error
	Close() error
	Error() error
	Submit(op RingOp) (OpID, error)
}

type OpID uint64

type RingOpType int8

const (
	RingOpAppend RingOpType = iota
)

type RingOp struct {
	ID            OpID
	Type          RingOpType
	WriteCategory DiskWriteCategory
	// Either Fd or File must be set.
	Fd         uintptr
	File       File
	DataBuffer Buffer
	Data       []byte
}
