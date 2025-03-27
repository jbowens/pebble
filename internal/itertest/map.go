// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package itertest

import "github.com/cockroachdb/pebble/internal/base"

// Map returns a new iterator that applies the provided function to each
// key/value pair returned by the original iterator. The provided function is
// invoked even when the iterator returns nil.
func Map(
	iter base.InternalIterator, fn func(*base.InternalKV) *base.InternalKV,
) base.InternalIterator {
	return Attach(iter, ProbeState{}, &mapProbe{fn})
}

type mapProbe struct {
	fn func(*base.InternalKV) *base.InternalKV
}

// Assert that mapProbe implements Probe.
var _ Probe = (*mapProbe)(nil)

func (p *mapProbe) Probe(pctx *ProbeContext) {
	pctx.Return.KV = p.fn(pctx.Return.KV)
}
