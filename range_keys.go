// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

const rangeKeyArenaSize = 1 << 20

// RangeValueMerger ...
type RangeValueMerger interface {
	Name() string
	MergeValues(dst []byte, includesBase bool, operands ...[]byte) []byte
}

func (d *DB) applyBatchRangeKeys(b *Batch) error {
	// Lazily construct the global range key arena, so that tests that
	// don't use range keys don't need to allocate this long-lived
	// buffer.
	d.rangeKeys.once.Do(func() {
		d.rangeKeys.arenaBuf = make([]byte, rangeKeyArenaSize)
		arena := arenaskl.NewArena(d.rangeKeys.arenaBuf)
		d.rangeKeys.skl.Reset(arena, d.cmp)
	})

	seqNum := b.SeqNum()
	startSeqNum := seqNum
	for r := b.Reader(); ; seqNum++ {
		kind, ukey, value, ok := r.Next()
		if !ok {
			break
		}
		if kind == InternalKeyKindLogData {
			// Don't increment seqNum for LogData, since these are not applied
			// to the memtable.
			seqNum--
			continue
		}
		if kind != InternalKeyKindRangeKey {
			continue
		}
		ikey := base.MakeInternalKey(ukey, seqNum, kind)
		if err := d.rangeKeys.skl.Add(ikey, value); err != nil {
			return err
		}
	}
	if seqNum != startSeqNum+uint64(b.Count()) {
		return base.CorruptionErrorf("pebble: inconsistent batch count: %d vs %d",
			errors.Safe(seqNum), errors.Safe(startSeqNum+uint64(b.Count())))
	}
	d.rangeKeys.cache.invalidate(uint32(b.countRangeKeys))
	return nil
}

func (d *DB) newRangeKeyIter(*IterOptions) *keyspan.Iter {
	rangeKeys := d.rangeKeys.cache.get()
	if rangeKeys == nil {
		return nil
	}
	return keyspan.NewIter(d.cmp, rangeKeys)
}

func (i *Iterator) ExperimentalRangeValue() RangeValue {
	// TODO(jackson): Maintain the range key iter's position through
	// normal Iterator options, or at least don't iterate through all
	// fragments every time we're asked for the current iterator
	// position's range value. It's a little awkward to use SeekGE here.
	// If there exists a fragment with a start key equal to the search
	// key, we'll land on the first of possibly many fragments with
	// those bounds and need to iterate forward and combine them.
	// Otherwise, all relevant fragments are behind the search key and
	// we need iterate backwards to combine them.
	//
	// I need to revisit how our range deletion iteration works.

	startKey, endKey := i.rangeKeyIter.First()
	for startKey != nil && i.cmp(endKey, i.key) <= 0 {
		startKey, endKey = i.rangeKeyIter.Next()
	}

	// NB: This function should return a RangeValue containing a single
	// span right now, because all range keys are fragmented within a
	// single global level. Only one set of fragment bounds will
	// overlap. In a real implementation where range keys descend
	// levels, fragments in different levels will have different bounds,
	// and each of those bounds will surface as a unique RangeKeySpan.

	var spans []RangeKeySpan
	for startKey != nil && i.cmp(startKey.UserKey, i.key) < 0 {
		if startKey.SeqNum() > i.seqNum {
			// Inserted later than our seq num.
			startKey, endKey = i.rangeKeyIter.Next()
			continue
		}

		spanValue := i.rangeKeyIter.SpanValue()
		if len(spans) == 0 {
			spans = append(spans, RangeKeySpan{
				Start: startKey.UserKey,
				End:   endKey,
				Value: spanValue,
			})
		} else {
			spans[len(spans)-1].Value = i.rangeMerge.MergeValues(
				nil, false, spans[len(spans)-1].Value, spanValue)
		}
		startKey, endKey = i.rangeKeyIter.Next()
	}
	return RangeValue{spans: spans}
}

type RangeValue struct {
	merger RangeValueMerger
	spans  []RangeKeySpan
}

type RangeKeySpan struct {
	Start []byte
	End   []byte
	Value []byte
}

func (rv RangeValue) Empty() bool {
	return len(rv.spans) == 0
}

func (rv RangeValue) MergedValue() []byte {
	switch len(rv.spans) {
	case 0:
		return nil
	default:
		merged := rv.spans[0].Value
		for i := range rv.spans {
			merged = rv.merger.MergeValues(nil, len(rv.spans)-1 == i, merged, rv.spans[i].Value)
		}
		return merged
	}
}
