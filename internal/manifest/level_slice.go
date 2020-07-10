// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
)

type LevelSlice interface {
	Len() int
	Iter() FileIterator
	Collect() []*FileMetadata
	KeyRange(Compare) (InternalKey, InternalKey)
}

var _ LevelSlice = sliceL0{}
var _ LevelSlice = sliceLN{}

type sliceL0 struct {
	files []*FileMetadata
}

func (s sliceL0) Len() int {
	return len(s.files)
}

func (s sliceL0) Iter() FileIterator {
	return FileIterator{files: s.files}
}

func (s sliceL0) Collect() []*FileMetadata {
	return s.files
}

func (s sliceL0) KeyRange(cmp base.Compare) (smallest InternalKey, largest InternalKey) {
	return KeyRange(cmp, s.files, nil)
}

type sliceLN struct {
	start FileIterator
	end   FileIterator
}

func (s sliceLN) Len() int {
	return s.Iter().Len()
}

func (s sliceLN) Iter() FileIterator {
	return FileIterator{
		files: s.start.files[s.start.cur:s.end.cur],
	}
}

func (s sliceLN) Collect() []*FileMetadata {
	return s.start.files[:s.end.cur]
}

func (s sliceLN) KeyRange(cmp base.Compare) (smallest InternalKey, largest InternalKey) {
	if cur := s.start.Current(); cur != nil {
		smallest = cur.Smallest
		largest = cur.Largest
	}
	if m := s.end.PeekPrev(); m != nil {
		largest = m.Largest
	}
	return smallest, largest
}

func SliceFileIterator(files []*FileMetadata) FileIterator {
	return FileIterator{files: files}
}

type FileIterator struct {
	files []*FileMetadata
	cur   int
}

func (i FileIterator) Len() int {
	return len(i.files) - i.cur
}

func (i FileIterator) Current() *FileMetadata {
	if !i.Valid() {
		return nil
	}
	return i.files[i.cur]
}

func (i FileIterator) Valid() bool {
	return i.cur >= 0 && i.cur < len(i.files)
}

func (i *FileIterator) First() *FileMetadata {
	i.cur = 0
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}

func (i *FileIterator) Last() *FileMetadata {
	if len(i.files) == 0 {
		return nil
	}
	i.cur = len(i.files) - 1
	return i.files[i.cur]
}

func (i *FileIterator) Next() *FileMetadata {
	i.cur++
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}

func (i FileIterator) PeekNext() *FileMetadata {
	if i.cur+1 >= len(i.files) {
		return nil
	}
	return i.files[i.cur+1]
}

func (i *FileIterator) Prev() *FileMetadata {
	i.cur--
	if i.cur < 0 {
		return nil
	}
	return i.files[i.cur]
}

func (i FileIterator) PeekPrev() *FileMetadata {
	if i.cur-1 < 0 {
		return nil
	}
	return i.files[i.cur-1]
}

func (i *FileIterator) SeekGE(cmp Compare, userKey []byte) *FileMetadata {
	i.cur = sort.Search(len(i.files), func(j int) bool {
		return cmp(userKey, i.files[j].Largest.UserKey) <= 0
	})
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}
