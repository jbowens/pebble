// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// positionIterator defines an internal iterator interface used by the merging
// iter to step through positions within the keyspace. A positionIterator may
// stop at positions within the keyspace at which no real key exists. A
// positionIterator returns iterKey structs, encapsulating key-value pair and an
// iterKeyKind enum indicating what type of position is represented (eg, point
// key, sstable boundary, etc)
//
// Much of the codebase implements and consumes the base.InternalIterator
// interface. That interface models an iterator over internal keys set by the
// user. The merging iterator is unique in that it sometimes needs to stop at
// keys/positions that aren't real keys set by the user. The levelIter iterator
// needs to be able to surface positions, such as sstable bounadries, to the
// merging iterator. This can't cleanly be modeled within the InternalIterator
// interface.
//
// To accommodate this, the merging iterator requires the iterators that it
// merges implement positionIterator instead. This allows the merged iterators
// to surface iterator positions of interest (such as sstable boundaries) to the
// merging iterator, with a kind enum to indicate that the position does not
// represent a real key.
//
// Internal iterators that represent simple iterators over points may be
// converted into position iterators through the fixedKindIterator adapter type.
type positionIterator interface {
	SeekGE(key []byte) iterKey
	SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) iterKey
	SeekLT(key []byte) iterKey
	First() iterKey
	Last() iterKey
	Next() iterKey
	Prev() iterKey
	Error() error
	Close() error
	SetBounds(lower, upper []byte)
	fmt.Stringer
}

type iterKeyKind int8

const (
	iterKeyNone iterKeyKind = iota
	iterKeyPoint
	iterKeyBoundary
	iterKeyIterBoundsSyntheticBoundary
)

func pointKey(k *InternalKey, v []byte) iterKey {
	return iterKey{k: k, v: v, kind: iterKeyPoint}
}

type iterKey struct {
	k    *InternalKey
	v    []byte
	kind iterKeyKind
}

func (k iterKey) String() string {
	switch k.kind {
	case iterKeyNone:
		return "(none)"
	case iterKeyPoint:
		return k.k.String()
	default:
		panic(fmt.Sprintf("unrecognized iterKeyKind %v", k.kind))
	}
}

func (k iterKey) kv() (*InternalKey, []byte) {
	return k.k, k.v
}

func pointIterator(ii internalIterator) positionIterator {
	return fixedKindIterator{kind: iterKeyPoint, internalIterator: ii}
}

// Assert that fixedKindIterator implements the positionIterator interface.
var _ positionIterator = fixedKindIterator{}

// fixedKindIterator wraps an internal iterator, surfacing each of its keys as
// iterKeys with the configured kind. It's used an adapter when an
// internalIterator needs to be used as a positionIterator by the merging
// iterator.
type fixedKindIterator struct {
	kind iterKeyKind
	internalIterator
}

func (fi fixedKindIterator) wrap(k *base.InternalKey, v []byte) iterKey {
	if k == nil {
		return iterKey{}
	}
	return iterKey{k: k, v: v, kind: fi.kind}
}

func (fi fixedKindIterator) SeekGE(key []byte) iterKey {
	return fi.wrap(fi.internalIterator.SeekGE(key))
}

func (fi fixedKindIterator) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) iterKey {
	return fi.wrap(fi.internalIterator.SeekPrefixGE(prefix, key, trySeekUsingNext))
}

func (fi fixedKindIterator) SeekLT(key []byte) iterKey {
	return fi.wrap(fi.internalIterator.SeekLT(key))
}

func (fi fixedKindIterator) First() iterKey {
	return fi.wrap(fi.internalIterator.First())
}

func (fi fixedKindIterator) Last() iterKey {
	return fi.wrap(fi.internalIterator.Last())
}

func (fi fixedKindIterator) Next() iterKey {
	return fi.wrap(fi.internalIterator.Next())
}

func (fi fixedKindIterator) Prev() iterKey {
	return fi.wrap(fi.internalIterator.Prev())
}

// iteratorTODO is a temporary function that converts an internalIterator into a
// positionIterator. The returned positionIterator surfaces keys as 'points'.
// TODO(jackson): Audit all of the callsites, either converting them to use an
// appropriate positionIterator implementation directly.
func iteratorTODO(iter internalIterator) positionIterator {
	return fixedKindIterator{kind: iterKeyPoint, internalIterator: iter}
}
