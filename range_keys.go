// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// constructRangeKeyIter constructs the range-key iterator stack, populating
// i.rangeKey.rangeKeyIter with the resulting iterator.
func (i *Iterator) constructRangeKeyIter() {
	i.rangeKey.rangeKeyIter = i.rangeKey.iterConfig.Init(i.cmp, i.seqNum)

	// If there's an indexed batch with range keys, include it.
	if i.batch != nil {
		if i.batch.index == nil {
			i.rangeKey.iterConfig.AddLevel(newErrorKeyspanIter(ErrNotIndexed))
		} else {
			// Only include the batch's range key iterator if it has any keys.
			// NB: This can force reconstruction of the rangekey iterator stack
			// in SetOptions if subsequently range keys are added. See
			// SetOptions.
			if i.batch.countRangeKeys > 0 {
				i.batch.initRangeKeyIter(&i.opts, &i.batchRangeKeyIter, i.batchSeqNum)
				i.rangeKey.iterConfig.AddLevel(&i.batchRangeKeyIter)
			}
		}
	}

	// Next are the flushables: memtables and large batches.
	for j := len(i.readState.memtables) - 1; j >= 0; j-- {
		mem := i.readState.memtables[j]
		// We only need to read from memtables which contain sequence numbers older
		// than seqNum.
		if logSeqNum := mem.logSeqNum; logSeqNum >= i.seqNum {
			continue
		}
		if rki := mem.newRangeKeyIter(&i.opts); rki != nil {
			i.rangeKey.iterConfig.AddLevel(rki)
		}
	}

	current := i.readState.current
	// Next are the file levels: L0 sub-levels followed by lower levels.
	//
	// Add file-specific iterators for L0 files containing range keys. This is less
	// efficient than using levelIters for sublevels of L0 files containing
	// range keys, but range keys are expected to be sparse anyway, reducing the
	// cost benefit of maintaining a separate L0Sublevels instance for range key
	// files and then using it here.
	iter := current.RangeKeyLevels[0].Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		spanIterOpts := &keyspan.SpanIterOptions{RangeKeyFilters: i.opts.RangeKeyFilters}
		spanIter, err := i.newIterRangeKey(f, spanIterOpts)
		if err != nil {
			i.rangeKey.iterConfig.AddLevel(&errorKeyspanIter{err: err})
			continue
		}
		i.rangeKey.iterConfig.AddLevel(spanIter)
	}

	// Add level iterators for the non-empty non-L0 levels.
	for level := 1; level < len(current.RangeKeyLevels); level++ {
		if current.RangeKeyLevels[level].Empty() {
			continue
		}
		li := i.rangeKey.iterConfig.NewLevelIter()
		spanIterOpts := keyspan.SpanIterOptions{RangeKeyFilters: i.opts.RangeKeyFilters}
		li.Init(spanIterOpts, i.cmp, i.newIterRangeKey, current.RangeKeyLevels[level].Iter(),
			manifest.Level(level), i.opts.logger, manifest.KeyTypeRange)
		i.rangeKey.iterConfig.AddLevel(li)
	}
}

type lazyCombinedIter struct {
	// parent holds a pointer to the root *pebble.Iterator containing this
	// iterator. It's used to mutate the internalIterator in use when switching
	// to combined iteration.
	parent            *Iterator
	pointIter         internalIteratorWithStats
	combinedIterState combinedIterState
}

type combinedIterState struct {
	key         []byte
	initialized bool
	triggered   bool
}

// Assert that *lazyCombinedIter implements internalIterator.
var _ internalIterator = (*lazyCombinedIter)(nil)

type SeekGEFlags = base.SeekGEFlags

func (i *lazyCombinedIter) initCombinedIteration(dir int8, pointKey *InternalKey, pointValue []byte, seekKey []byte) (*InternalKey, []byte) {
	// Invariant: i.rangeKey is nil.
	// Invariant: !i.initialized.

	// An operation on the point iterator observed a file containing range keys,
	// so we must switch to combined interleaving iteration. First, construct
	// the range key iterator stack. It must not exist, otherwise we'd already
	// be performing combined iteration.
	i.parent.rangeKey = iterRangeKeyStateAllocPool.Get().(*iteratorRangeKeyState)
	i.parent.rangeKey.init(i.parent.cmp, i.parent.split, &i.parent.opts)
	i.parent.constructRangeKeyIter()

	// Initialize the Iterator's interleaving iterator.
	i.parent.rangeKey.iiter.Init(
		i.parent.cmp, i.parent.pointIter, i.parent.rangeKey.rangeKeyIter,
		i.parent.rangeKey, i.parent.opts.LowerBound, i.parent.opts.UpperBound)

	// We need to determine the key to seek the range key iterator to. If
	// seekKey is not nil, the operation that triggered the switch to combined
	// iteration was a seek, and we use that key. Otherwise, a relative
	// positioning operation triggered the switch to combined iteration.
	//
	// The levelIter that observed a file containing range keys populated
	// combinedIterState.key with an appropriate seek key for the direction of
	// iteration. If multiple levelIters observed files with range keys during
	// the same operation on the mergingIter, combinedIterState.key is the
	// smallest [during forward iteration; largest in reverse iteration] such
	// key, ensuring the seek finds the earliest appropriate range key.
	if seekKey == nil {
		seekKey = i.combinedIterState.key
		if dir == +1 {
			if i.parent.opts.LowerBound != nil && i.parent.cmp(i.combinedIterState.key, i.parent.opts.LowerBound) < 0 {
				seekKey = i.parent.opts.LowerBound
			} else if pointKey != nil && i.parent.cmp(i.combinedIterState.key, pointKey.UserKey) > 0 {
				seekKey = pointKey.UserKey
			}
		}
		if dir == -1 {
			if dir == -1 && i.parent.opts.UpperBound != nil && i.parent.cmp(seekKey, i.parent.opts.UpperBound) > 0 {
				seekKey = i.parent.opts.UpperBound
			} else if pointKey != nil && i.parent.cmp(seekKey, pointKey.UserKey) < 0 {
				seekKey = pointKey.UserKey
			}
		}
	}

	// Set the parent's primary iterator to point to the combined, interleaving
	// iterator that's now initialized with our current state.
	i.parent.iter = &i.parent.rangeKey.iiter
	i.combinedIterState.initialized = true
	i.combinedIterState.key = nil

	// All future iterator operations will go directly through the combined
	// iterator.
	if dir == +1 {
		return i.parent.rangeKey.iiter.InitSeekGE(seekKey, pointKey, pointValue)
	} else {
		return i.parent.rangeKey.iiter.InitSeekLT(seekKey, pointKey, pointValue)
	}
}

func (i *lazyCombinedIter) SeekGE(key []byte, flags base.SeekGEFlags) (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.SeekGE(key, flags)
	}
	k, v := i.pointIter.SeekGE(key, flags)
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(+1, k, v, key)
	}
	return k, v
}

func (i *lazyCombinedIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.SeekPrefixGE(prefix, key, flags)
	}
	k, v := i.pointIter.SeekPrefixGE(prefix, key, flags)
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(+1, k, v, key)
	}
	return k, v
}

func (i *lazyCombinedIter) SeekLT(key []byte, flags base.SeekLTFlags) (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.SeekLT(key, flags)
	}
	k, v := i.pointIter.SeekLT(key, flags)
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(-1, k, v, key)
	}
	return k, v
}

func (i *lazyCombinedIter) First() (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.First()
	}
	k, v := i.pointIter.First()
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(+1, k, v, nil)
	}
	return k, v
}

func (i *lazyCombinedIter) Last() (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Last()
	}
	k, v := i.pointIter.Last()
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(-1, k, v, nil)
	}
	return k, v
}

func (i *lazyCombinedIter) Next() (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Next()
	}
	k, v := i.pointIter.Next()
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(+1, k, v, nil)
	}
	return k, v
}

func (i *lazyCombinedIter) Prev() (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Prev()
	}
	k, v := i.pointIter.Prev()
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(-1, k, v, nil)
	}
	return k, v
}

func (i *lazyCombinedIter) Error() error {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Error()
	}
	return i.pointIter.Error()
}

func (i *lazyCombinedIter) Close() error {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Close()
	}
	return i.pointIter.Close()
}

func (i *lazyCombinedIter) SetBounds(lower, upper []byte) {
	if i.combinedIterState.initialized {
		i.parent.rangeKey.iiter.SetBounds(lower, upper)
		return
	}
	i.pointIter.SetBounds(lower, upper)
}

func (i *lazyCombinedIter) String() string {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.String()
	}
	return i.pointIter.String()
}

func (i *lazyCombinedIter) Stats() InternalIteratorStats {
	return i.pointIter.Stats()
}

func (i *lazyCombinedIter) ResetStats() {
	i.pointIter.ResetStats()
}
