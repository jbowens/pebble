// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// KeyFilter is used in an Iterator to filter individual keys within an sstable.
// It should not maintain any per-sstable state, and must be thread-safe. A
// KeyFilter is required to always return the same result for a given key,
// otherwise an Iterator may yield inconsistent results.
type KeyFilter interface {
	// IsVisible returns true if the key should be visible to the iterator, and
	// false if it should be filtered.
	IsVisible(key []byte) (bool, error)
}

// KeyFilterFunc wraps a function, implementing the KeyFilter interface.
type KeyFilterFunc func([]byte) (bool, error)

// IsVisible returns true if the key should be visible to the iterator, and
// false if it should be filtered.
func (f KeyFilterFunc) IsVisible(key []byte) (bool, error) {
	return f(key)
}

type filterIter struct {
	err       error
	KeyFilter KeyFilter
	Iterator  internalIterator
}

var _ internalIterator = (&filterIter{})

func (fi *filterIter) filterForward(k *InternalKey, v []byte) (*InternalKey, []byte) {
	fi.err = nil
	var passFilter bool
	for k != nil && !passFilter {
		passFilter, fi.err = fi.KeyFilter.IsVisible(k.UserKey)
		if fi.err != nil {
			return nil, nil
		}
		if !passFilter {
			k, v = fi.Iterator.Next()
		}
	}
	return k, v
}

func (fi *filterIter) filterBackward(k *InternalKey, v []byte) (*InternalKey, []byte) {
	fi.err = nil
	var passFilter bool
	for k != nil && !passFilter {
		passFilter, fi.err = fi.KeyFilter.IsVisible(k.UserKey)
		if fi.err != nil {
			return nil, nil
		}
		if !passFilter {
			k, v = fi.Iterator.Prev()
		}
	}
	return k, v
}

func (fi *filterIter) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	return fi.filterForward(fi.Iterator.SeekGE(key, trySeekUsingNext))
}

func (fi *filterIter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	return fi.filterForward(fi.Iterator.SeekPrefixGE(prefix, key, trySeekUsingNext))
}

func (fi *filterIter) SeekLT(key []byte) (*InternalKey, []byte) {
	return fi.filterBackward(fi.Iterator.SeekLT(key))
}

func (fi *filterIter) First() (*InternalKey, []byte) {
	return fi.filterForward(fi.Iterator.First())
}

func (fi *filterIter) Last() (*InternalKey, []byte) {
	return fi.filterBackward(fi.Iterator.Last())
}

func (fi *filterIter) Next() (*InternalKey, []byte) {
	return fi.filterForward(fi.Iterator.Next())
}

func (fi *filterIter) Prev() (*InternalKey, []byte) {
	return fi.filterBackward(fi.Iterator.Prev())
}

func (fi *filterIter) Error() error {
	if fi.err != nil {
		return fi.err
	}
	return fi.Iterator.Error()
}

func (fi *filterIter) Close() error {
	return fi.Iterator.Close()
}

func (fi *filterIter) SetBounds(lower, upper []byte) {
	fi.Iterator.SetBounds(lower, upper)
}

func (fi *filterIter) String() string {
	return fi.Iterator.String()
}
