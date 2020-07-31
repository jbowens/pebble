// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// LevelMetadata contains metadata for all of the files within
// a level of the LSM.
type LevelMetadata struct {
	tree btree
}

// Len returns the number of files within the level.
func (lm *LevelMetadata) Len() int {
	return lm.tree.length
}

// Iter constructs a LevelIterator over the entire level.
func (lm *LevelMetadata) Iter() LevelIterator {
	return LevelIterator{iter: lm.tree.MakeIter()}
}

// Slice constructs a slice containing the entire level.
func (lm *LevelMetadata) Slice() LevelSlice {
	return LevelSlice{start: lm.tree.MakeIter()}
}

// LevelFile holds a file's metadata along with its position
// within a level of the LSM.
type LevelFile struct {
	*FileMetadata
	slice LevelSlice
}

// Slice constructs a LevelSlice containing only this file.
func (lf LevelFile) Slice() LevelSlice {
	return lf.slice
}

type Ordering struct {
	name string
	cmp  func(*FileMetadata, *FileMetadata) int
}

func KeyOrdering(cmp Compare) Ordering {
	return Ordering{
		name: "key",
		cmp:  func(a, b *FileMetadata) int { return a.cmpSmallestKey(b, cmp) },
	}
}

var SeqOrdering = Ordering{
	name: "seq",
	cmp:  func(a, b *FileMetadata) int { return a.cmpSeqNum(b) },
}

// NewLevelSlice constructs a LevelSlice over the provided files. This
// function is expected to be a temporary adapter between interfaces.
// TODO(jackson): Revisit once the conversion of Version.Files to a btree is
// complete.
func NewLevelSlice(files []*FileMetadata, order Ordering) LevelSlice {
	t := new(btree)
	t.cmp = order.cmp
	for _, f := range files {
		t.Set(f)
	}
	if invariants.Enabled {
		if t.length != len(files) {
			panic(errors.New("duplicates passed to NewLevelSlice"))
		}
	}
	return LevelSlice{start: t.MakeIter()}
}

// LevelSlice contains a slice of the files within a level of the LSM.
type LevelSlice struct {
	// start and end form the inclusive bounds of a slice of files within a
	// level of the LSM.
	start iterator
	end   iterator
}

// Each invokes fn for each element in the slice.
func (ls LevelSlice) Each(fn func(*FileMetadata)) {
	iter := ls.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		fn(f)
	}
}

// Empty indicates whether the slice contains any files.
func (ls LevelSlice) Empty() bool {
	return ls.start.r == nil
}

// Iter constructs a LevelIterator that iterates over the slice.
func (ls LevelSlice) Iter() LevelIterator {
	return LevelIterator{
		start: &ls.start,
		end:   &ls.end,
	}
}

// Len returns the number of files in the slice.
func (ls LevelSlice) Len() int {
	// TODO(jackson): Avoid an O(n) scan by annotating the nodes of the
	// B-Tree.
	var count int
	iter := ls.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		count++
	}
	return count
}

// SizeSum sums the size of all files in the slice.
func (ls LevelSlice) SizeSum() uint64 {
	// TODO(jackson): Avoid an O(n) scan by annotating the nodes of the
	// B-Tree.
	var sum uint64
	iter := ls.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		sum += f.Size
	}
	return sum
}

// Reslice constructs a new slice backed by the same underlying level, with
// new start and end positions. Reslice invokes the provided function, passing
// two LevelIterators: one positioned to i's inclusive start and one
// positioned to i's inclusive end. The resliceFunc may move either iterator
// forward or backwards, including beyond the callee's original bounds to
// capture additional files from the underlying level. Reslice constructs and
// returns a new LevelSlice with the final bounds of the iterators after
// calling resliceFunc.
func (ls LevelSlice) Reslice(resliceFunc func(start, end *LevelIterator)) LevelSlice {
	// NB: We leave LevelIterator's start and end nil so the iterators are
	// unbounded in the underlying B-Tree.
	start := LevelIterator{iter: ls.start}
	end := LevelIterator{iter: ls.end}
	resliceFunc(&start, &end)
	return LevelSlice{
		start: start.iter,
		end:   end.iter,
	}
}

// LevelIterator iterates over a set of files' metadata. Its zero value is an
// empty iterator.
type LevelIterator struct {
	// iter is the current position of the LevelIterator.
	iter iterator
	// start and end, if non-nil, are the inclusive bounds of files visible to
	// the LevelIterator. Typically, these pointers point into a LevelSlice's
	// start and end fields. These iterators must not be modified.
	start *iterator
	end   *iterator
}

// Clone copies the iterator, returning an independent iterator at the same
// position with the same bounds.
func (i *LevelIterator) Clone() LevelIterator {
	// The start and end iterators are not cloned and are treated as
	// immutable.
	return LevelIterator{
		iter:  i.iter.clone(),
		start: i.start,
		end:   i.end,
	}
}

// Empty indicates whether there are remaining files in the iterator.
func (i LevelIterator) Empty() bool {
	return i.iter.r == nil
}

// Current returns the item at the current iterator position.
func (i LevelIterator) Current() *FileMetadata {
	if !i.iter.Valid() {
		return nil
	}
	return i.iter.Cur()
}

// First seeks to the first file in the iterator and returns it.
func (i *LevelIterator) First() *FileMetadata {
	i.iter.First()
	return i.Current()
}

// Last seeks to the last file in the iterator and returns it.
func (i *LevelIterator) Last() *FileMetadata {
	i.iter.Last()
	return i.Current()
}

// Next advances the iterator to the next file and returns it.
func (i *LevelIterator) Next() *FileMetadata {
	i.iter.Next()
	return i.Current()
}

// Prev moves the iterator the previous file and returns it.
func (i *LevelIterator) Prev() *FileMetadata {
	i.iter.Prev()
	return i.Current()
}

// SeekGE seeks to the first file in the iterator's file set with a largest
// user key less than or equal to the provided user key. The iterator must
// have been constructed from L1+, because it requires the underlying files to
// be sorted by user keys and non-overlapping.
func (i *LevelIterator) SeekGE(cmp Compare, userKey []byte) *FileMetadata {
	i.iter.Seek(func(m *FileMetadata) int {
		return cmp(userKey, m.Largest.UserKey)
	})
	return i.iter.Cur()
	//files := i.files[i.start:i.end]
	//i.cur = i.start + sort.Search(len(files), func(j int) bool {
	//return cmp(userKey, files[j].Largest.UserKey) <= 0
	//})
	//if i.cur >= i.end {
	//return nil
	//}
	//return i.files[i.cur]
}

// SeekLT seeks to the last file in the iterator's file set with a smallest
// user key less than the provided user key. The iterator must have been
// constructed from L1+, because it requries the underlying files to be sorted
// by user keys and non-overlapping.
func (i *LevelIterator) SeekLT(cmp Compare, userKey []byte) *FileMetadata {
	i.iter.Seek(func(m *FileMetadata) int {
		return cmp(m.Smallest.UserKey, userKey)
	})
	return i.iter.Cur()
	//files := i.files[i.start:i.end]
	//i.cur = i.start + sort.Search(len(files), func(j int) bool {
	//return cmp(files[j].Smallest.UserKey, userKey) >= 0
	//})
	//if i.cur < i.start {
	//return nil
	//}
	//return i.Prev()
}

// Take constructs a LevelFile containing the file at the iterator's current
// position. Take panics if the iterator is not currently positioned over a
// file.
func (i LevelIterator) Take() LevelFile {
	m := i.Current()
	if m == nil {
		panic("Take called on invalid LevelIterator")
	}

	return LevelFile{
		FileMetadata: m,
		slice: LevelSlice{
			start: i.iter,
			end:   i.iter,
		},
	}
}
