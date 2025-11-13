// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
)

type posixIORing struct {
	fs   FS
	err  atomic.Value
	done chan struct{}
	wg   sync.WaitGroup
	mu   struct {
		sync.Mutex
		lastSubmittedOpID OpID
		cond              sync.Cond
		pending           []RingOp
		files             map[File]struct{}
	}
}

func newPosixIORing(fs FS) *posixIORing {
	r := &posixIORing{
		fs:   fs,
		done: make(chan struct{}),
	}
	r.mu.cond.L = &r.mu.Mutex
	r.wg.Go(r.runLoop)
	return r
}

// Assert that *posixIORing implements vfs.IORing.
var _ IORing = (*posixIORing)(nil)

func (r *posixIORing) Buffer(n int) Buffer {
	return Buffer{b: make([]byte, n)}
}

func (r *posixIORing) Submit(op RingOp) (OpID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.lastSubmittedOpID++
	id := r.mu.lastSubmittedOpID
	op.ID = id
	r.mu.pending = append(r.mu.pending, op)
	r.mu.cond.Broadcast()
	r.mu.files[op.File] = struct{}{}
	return id, nil
}

func (r *posixIORing) Error() error {
	err := r.err.Load()
	if err == nil {
		return nil
	}
	return err.(error)
}

func (r *posixIORing) Sync() error {
	panic("TODO")
}

func (r *posixIORing) Close() error {
	close(r.done)
	r.mu.Lock()
	r.mu.cond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
	return r.Error()
}

func (r *posixIORing) recordErr(op RingOp, err error) {
	err = errors.Wrapf(err, "op %d", op.ID)
	existingErr := r.err.Load()
	if existingErr == nil {
		r.err.Store(err)
		return
	}
	// Single writer; lack of atomicity is ok.
	r.err.Store(errors.CombineErrors(err, existingErr.(error)))
}

func (r *posixIORing) runLoop() {
	for {
		op, done := func() (op RingOp, done bool) {
			r.mu.Lock()
			defer r.mu.Unlock()
			for len(r.mu.pending) == 0 {
				select {
				case <-r.done:
					return RingOp{}, true
				default:
					// Wait for the next operation to be submitted.
				}
				r.mu.cond.Wait()
			}
			op, r.mu.pending = r.mu.pending[0], r.mu.pending[1:]
			return op, false
		}()
		if done {
			return
		}
		switch op.Type {
		case RingOpAppend:
			_, err := op.File.Write(op.Data)
			if err != nil {
				r.recordErr(op, err)
			}
			// TODO(jackson): Recycle op.DataBuffer if set.
		default:
			panic(errors.AssertionFailedf("unexpected op type: %d", op.Type))
		}
	}
}
