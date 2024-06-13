// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

type ColumnarWriter struct {
	comparer *base.Comparer
	writable objstorage.Writable
	meta     WriterMetadata
	opts     WriterOptions
	// dataBlockOptions and indexBlockOptions are used to configure the sstable
	// block flush heuristics.
	dataBlockOptions     flushDecisionOptions
	indexBlockOptions    flushDecisionOptions
	allocatorSizeClasses []int
	wg                   sync.WaitGroup
	err                  error
	props                Properties
	// block writers buffering unflushed data.
	dataBlock          colblk.DataBlockWriter
	indexBlock         colblk.IndexBlockWriter
	topLevelIndexBlock colblk.IndexBlockWriter
	rangeDelBlock      colblk.KeyspanBlockWriter
	rangeKeyBlock      colblk.KeyspanBlockWriter
	valueBlock         *valueBlockWriter
	// filter accumulates the filter block. If populated, the filter ingests
	// either the output of w.split (i.e. a prefix extractor) if w.split is not
	// nil, or the full keys otherwise.
	filterBlock  filterWriter
	prevPointKey struct {
		trailer    base.InternalKeyTrailer
		isObsolete bool
	}
	dataBlockSize  int
	indexBlockSize int
	size           uint64
	writeQueue     chan *compressedBlock

	separatorBuf          []byte
	tmp                   []byte
	tmpValuePrefix        [1]byte
	disableKeyOrderChecks bool
}

func NewColumnarWriter(
	writable objstorage.Writable, o WriterOptions, extraOpts ...WriterOption,
) *ColumnarWriter {
	if writable == nil {
		panic("pebble: nil writable")
	}
	o = o.ensureDefaults()
	w := &ColumnarWriter{
		comparer: o.Comparer,
		writable: writable,
		meta: WriterMetadata{
			SmallestSeqNum: math.MaxUint64,
		},
		dataBlockOptions: flushDecisionOptions{
			blockSize:               o.BlockSize,
			blockSizeThreshold:      (o.BlockSize*o.BlockSizeThreshold + 99) / 100,
			sizeClassAwareThreshold: (o.BlockSize*o.SizeClassAwareThreshold + 99) / 100,
		},
		indexBlockOptions: flushDecisionOptions{
			blockSize:               o.IndexBlockSize,
			blockSizeThreshold:      (o.IndexBlockSize*o.BlockSizeThreshold + 99) / 100,
			sizeClassAwareThreshold: (o.IndexBlockSize*o.SizeClassAwareThreshold + 99) / 100,
		},
		allocatorSizeClasses: o.AllocatorSizeClasses,
		opts:                 o,
		writeQueue:           make(chan *compressedBlock),
	}
	w.dataBlock.Init(o.KeySchema)
	w.indexBlock.Init()
	w.topLevelIndexBlock.Init()
	w.rangeDelBlock.Init(w.comparer.Equal)
	w.rangeKeyBlock.Init(w.comparer.Equal)
	if !o.DisableValueBlocks {
		w.valueBlock = newValueBlockWriter(
			w.dataBlockOptions.blockSize, w.dataBlockOptions.blockSizeThreshold,
			w.opts.Compression, w.opts.Checksum, func(compressedSize int) {
				panic("TODO")
				// w.coordination.sizeEstimate.dataBlockCompressed(compressedSize, 0)
			})
	}
	if o.FilterPolicy != nil {
		switch o.FilterType {
		case TableFilter:
			w.filterBlock = newTableFilterWriter(o.FilterPolicy)
		default:
			panic(fmt.Sprintf("unknown filter type: %v", o.FilterType))
		}
	}

	w.props.ComparerName = o.Comparer.Name
	w.props.CompressionName = o.Compression.String()
	w.props.MergerName = o.MergerName
	w.props.PropertyCollectorNames = "[]"
	w.wg.Add(1)
	go w.drainWriteQueue()
	return w
}

// EncodeSpan encodes the keys in the given span. The span can contain either
// only RANGEDEL keys or only range keys.
func (w *ColumnarWriter) EncodeSpan(span *keyspan.Span) error {
	if span.Empty() {
		return nil
	}
	if span.Keys[0].Kind() == base.InternalKeyKindRangeDelete {
		w.rangeDelBlock.AddSpan(span)
	}
	w.rangeKeyBlock.AddSpan(span)
	return nil
}

// Add adds a point key/value pair to the table being written. For a given
// Writer, the keys passed to Add must be in increasing order. Span keys (range
// deletions, range keys) must be added through EncodeSpan.
func (w *ColumnarWriter) Add(key InternalKey, value []byte) error {
	if w.opts.IsStrictObsolete {
		return errors.Errorf("use AddWithForceObsolete")
	}
	return w.AddWithForceObsolete(key, value, false)
}

// AddWithForceObsolete adds a point key/value pair when writing a
// strict-obsolete sstable. For a given Writer, the keys passed to Add must be
// in increasing order. Span keys (range deletions, range keys) must be added
// through EncodeSpan.
//
// forceObsolete indicates whether the caller has determined that this key is
// obsolete even though it may be the latest point key for this userkey. This
// should be set to true for keys obsoleted by RANGEDELs, and is required for
// strict-obsolete sstables.
//
// Note that there are two properties, S1 and S2 (see comment in format.go)
// that strict-obsolete ssts must satisfy. S2, due to RANGEDELs, is solely the
// responsibility of the caller. S1 is solely the responsibility of the
// callee.
func (w *ColumnarWriter) AddWithForceObsolete(
	key InternalKey, value []byte, forceObsolete bool,
) error {
	if w.err != nil {
		return w.err
	}
	switch key.Kind() {
	case base.InternalKeyKindRangeDelete, base.InternalKeyKindRangeKeySet,
		base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
		return errors.Newf("%s must be added through EncodeSpan", key.Kind())
	case base.InternalKeyKindMerge:
		if w.opts.IsStrictObsolete {
			return errors.Errorf("MERGE not supported in a strict-obsolete sstable")
		}
	}

	eval, err := w.evaluatePoint(key, len(value))
	if err != nil {
		return err
	}
	eval.isObsolete = eval.isObsolete || forceObsolete
	w.prevPointKey.trailer = key.Trailer
	w.prevPointKey.isObsolete = eval.isObsolete

	var valuePrefix []byte
	var valueStoredWithKey []byte
	if eval.writeToValueBlock {
		vh, err := w.valueBlock.addValue(value)
		if err != nil {
			return err
		}
		n := encodeValueHandle(w.tmp[:], vh)
		valueStoredWithKey = w.tmp[:n]
		var attribute base.ShortAttribute
		if w.opts.ShortAttributeExtractor != nil {
			// TODO(sumeer): for compactions, it is possible that the input sstable
			// already has this value in the value section and so we have already
			// extracted the ShortAttribute. Avoid extracting it again. This will
			// require changing the Writer.Add interface.
			if attribute, err = w.opts.ShortAttributeExtractor(
				key.UserKey, eval.kcmp.PrefixLen, value); err != nil {
				return err
			}
		}
		w.tmpValuePrefix[0] = byte(block.ValueHandlePrefix(eval.kcmp.PrefixEqual(), attribute))
		valuePrefix = w.tmpValuePrefix[:]
	} else {
		valueStoredWithKey = value
		if len(value) > 0 {
			w.tmpValuePrefix[0] = byte(block.InPlaceValuePrefix(eval.kcmp.PrefixEqual()))
			valuePrefix = w.tmpValuePrefix[:]
		}
	}

	// Append the key to the data block. Note we have not yet committed to
	// including the key in the block. The data block writer permits us to
	// finish the block excluding a tail of appended keys.
	entriesWithoutKV := w.dataBlock.Rows()
	w.dataBlock.Add(key, valuePrefix, valueStoredWithKey, eval.kcmp)

	// Now that we've appended the KV pair, we can compute the exact size of the
	// block with this key-value pair included. Check to see if we should flush
	// the current block, either with or without the added key-value pair.
	size := w.dataBlock.Size()
	if shouldFlushWithoutLatestKV(size, w.dataBlockSize, entriesWithoutKV, w.dataBlockOptions, w.allocatorSizeClasses) {
		// Flush the data block excluding the key we just added.
		if err := w.flushDataBlock(key.UserKey, eval.kcmp); err != nil {
			w.err = err
			return err
		}
		// flushDataBlock reset the data block builder, and we can add the key
		// to this next block now.
		w.dataBlock.Add(key, valuePrefix, valueStoredWithKey, eval.kcmp)
		w.dataBlockSize = w.dataBlock.Size()
	} else {
		// We're not flushing the data block, and we're committing to including
		// the current KV in the block. Remember the new size of the data block
		// with the current KV.
		w.dataBlockSize = size
	}

	/*
		for i := range w.blockPropCollectors {
			v := value
			if addPrefixToValueStoredWithKey {
				// Values for SET are not required to be in-place, and in the future may
				// not even be read by the compaction, so pass nil values. Block
				// property collectors in such Pebble DB's must not look at the value.
				v = nil
			}
			if err := w.blockPropCollectors[i].Add(key, v); err != nil {
				w.err = err
				return err
			}
		}
	*/
	// w.obsoleteCollector.AddPoint(eval.isObsolete)
	if w.filterBlock != nil {
		w.filterBlock.addKey(key.UserKey[:eval.kcmp.PrefixLen])
	}
	w.meta.updateSeqNum(key.SeqNum())

	if !w.meta.HasPointKeys {
		// NB: We need to ensure that SmallestPoint.UserKey is set, so we create
		// an InternalKey which is semantically identical to the key, but won't
		// have a nil UserKey. We do this, because key.UserKey could be nil, and
		// we don't want SmallestPoint.UserKey to be nil.
		//
		// todo(bananabrick): Determine if it's okay to have a nil SmallestPoint
		// .UserKey now that we don't rely on a nil UserKey to determine if the
		// key has been set or not.
		w.meta.SetSmallestPointKey(key.Clone())
	}

	w.props.NumEntries++
	switch key.Kind() {
	case InternalKeyKindDelete, InternalKeyKindSingleDelete:
		w.props.NumDeletions++
		w.props.RawPointTombstoneKeySize += uint64(len(key.UserKey))
	case InternalKeyKindDeleteSized:
		var size uint64
		if len(value) > 0 {
			var n int
			size, n = binary.Uvarint(value)
			if n <= 0 {
				w.err = errors.Newf("%s key's value (%x) does not parse as uvarint",
					errors.Safe(key.Kind().String()), value)
				return w.err
			}
		}
		w.props.NumDeletions++
		w.props.NumSizedDeletions++
		w.props.RawPointTombstoneKeySize += uint64(len(key.UserKey))
		w.props.RawPointTombstoneValueSize += size
	case InternalKeyKindMerge:
		w.props.NumMergeOperands++
	}
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(len(value))
	return nil
}

var compressedBlockPool = sync.Pool{
	New: func() interface{} {
		return new(compressedBlock)
	},
}

type compressedBlock struct {
	compressed []byte
	trailer    block.Trailer
	blockBuf   blockBuf
}

func (w *ColumnarWriter) flushDataBlock(nextKey []byte, kcmp colblk.KeyComparison) error {
	// Compute the separator that will be written to the index block alongside
	// this data block's end offset.
	w.separatorBuf = w.dataBlock.KeyWriter.SeparatePrev(w.separatorBuf[:0], nextKey, kcmp)
	sep := w.separatorBuf

	// Serialize the data block, compress it and send it to the write queue.
	uncompressed := w.dataBlock.Finish()
	w.dataBlock.Reset()
	cb := compressedBlockPool.Get().(*compressedBlock)
	cb.blockBuf.checksummer.Type = w.opts.Checksum
	cb.compressed, cb.trailer = compressAndChecksum(uncompressed, w.opts.Compression, &cb.blockBuf)
	w.size += uint64(len(cb.compressed))
	w.writeQueue <- cb

	// Add the separator to the index block. This might trigger a flush of the
	// index block too.
	i := w.indexBlock.AddDataBlockHandle(sep, w.size, uint64(len(cb.compressed)))
	sizeWithEntry := w.indexBlock.Size()
	if shouldFlushWithoutLatestKV(sizeWithEntry, w.indexBlockSize, i, w.indexBlockOptions, w.allocatorSizeClasses) {
		uncompressedIndexBlock := w.indexBlock.Finish()
		w.indexBlock.Reset()
		cb := compressedBlockPool.Get().(*compressedBlock)
		cb.blockBuf.checksummer.Type = w.opts.Checksum
		cb.compressed, cb.trailer = compressAndChecksum(uncompressedIndexBlock, w.opts.Compression, &cb.blockBuf)
		w.size += uint64(len(cb.compressed))
		w.writeQueue <- cb
		w.topLevelIndexBlock.AddIndexBlockHandle(sep, w.size, uint64(len(cb.compressed)))
	}
	return nil
}

type pointKeyEvaluation struct {
	kcmp              colblk.KeyComparison
	isObsolete        bool
	writeToValueBlock bool
}

func (w *ColumnarWriter) evaluatePoint(
	key base.InternalKey, valueLen int,
) (eval pointKeyEvaluation, err error) {
	if !w.meta.HasPointKeys {
		return pointKeyEvaluation{}, nil
	}
	keyKind := key.Kind()
	// Ensure that no one adds a point key kind without considering the obsolete
	// handling for that kind.
	switch keyKind {
	case InternalKeyKindSet, InternalKeyKindSetWithDelete, InternalKeyKindMerge,
		InternalKeyKindDelete, InternalKeyKindSingleDelete, InternalKeyKindDeleteSized:
	default:
		panic(errors.AssertionFailedf("unexpected key kind %s", keyKind.String()))
	}
	prevKeyKind := w.prevPointKey.trailer.Kind()
	eval.kcmp = w.dataBlock.KeyWriter.ComparePrev(key.UserKey)
	// If same user key, then the current key is obsolete if any of the
	// following is true:
	// C1 The prev key was obsolete.
	// C2 The prev key was not a MERGE. When the previous key is a MERGE we must
	//    preserve SET* and MERGE since their values will be merged into the
	//    previous key. We also must preserve DEL* since there may be an older
	//    SET*/MERGE in a lower level that must not be merged with the MERGE --
	//    if we omit the DEL* that lower SET*/MERGE will become visible.
	//
	// Regardless of whether it is the same user key or not
	// C3 The current key is some kind of point delete, and we are writing to
	//    the lowest level, then it is also obsolete. The correctness of this
	//    relies on the same user key not spanning multiple sstables in a level.
	//
	// C1 ensures that for a user key there is at most one transition from
	// !obsolete to obsolete. Consider a user key k, for which the first n keys
	// are not obsolete. We consider the various value of n:
	//
	// n = 0: This happens due to forceObsolete being set by the caller, or due
	// to C3. forceObsolete must only be set due a RANGEDEL, and that RANGEDEL
	// must also delete all the lower seqnums for the same user key. C3 triggers
	// due to a point delete and that deletes all the lower seqnums for the same
	// user key.
	//
	// n = 1: This is the common case. It happens when the first key is not a
	// MERGE, or the current key is some kind of point delete.
	//
	// n > 1: This is due to a sequence of MERGE keys, potentially followed by a
	// single non-MERGE key.
	isObsoleteC1AndC2 := eval.kcmp.UserKeyComparison == 0 &&
		(w.prevPointKey.isObsolete || prevKeyKind != InternalKeyKindMerge)
	isObsoleteC3 := w.opts.WritingToLowestLevel &&
		(keyKind == InternalKeyKindDelete || keyKind == InternalKeyKindSingleDelete ||
			keyKind == InternalKeyKindDeleteSized)
	eval.isObsolete = isObsoleteC1AndC2 || isObsoleteC3
	// TODO(sumeer): storing isObsolete SET and SETWITHDEL in value blocks is
	// possible, but requires some care in documenting and checking invariants.
	// There is code that assumes nothing in value blocks because of single MVCC
	// version (those should be ok). We have to ensure setHasSamePrefix is
	// correctly initialized here etc.

	if !w.disableKeyOrderChecks && (eval.kcmp.UserKeyComparison < 0 ||
		(eval.kcmp.UserKeyComparison == 0 && w.prevPointKey.trailer <= key.Trailer)) {
		return eval, errors.Errorf(
			"pebble: keys must be added in strictly increasing order: %s",
			key.Pretty(w.comparer.FormatKey))
	}

	// We might want to write this key's value to a value block if it has the
	// same prefix.
	//
	// We require:
	//  . Value blocks to be enabled.
	//  . The current key to have the same prefix as the previous key.
	//  . The previous key to be a SET.
	//  . The current key to be a SET.
	//  . If there are bounds requiring some keys' values to be in-place, the
	//    key must not fall within those bounds.
	//  . The value to be sufficiently large. (Currently we simply require a
	//    non-zero length, so all non-empty values are eligible for storage
	//    out-of-band in a value block.)
	if w.opts.DisableValueBlocks || !eval.kcmp.PrefixEqual() ||
		prevKeyKind != InternalKeyKindSet || keyKind == InternalKeyKindSet {
		return eval, nil
	}
	// NB: it is possible that eval.kcmp.UserKeyComparison == 0, i.e., these two
	// SETs have identical user keys (because of an open snapshot). This should
	// be the rare case.

	// Use of 0 here is somewhat arbitrary. Given the minimum 3 byte encoding of
	// valueHandle, this should be > 3. But tiny values are common in test and
	// unlikely in production, so we use 0 here for better test coverage.
	const tinyValueThreshold = 0
	if valueLen <= tinyValueThreshold {
		return eval, nil
	}

	// If there are bounds requiring some keys' values to be in-place, compare
	// the prefix against the bounds.
	if !w.opts.RequiredInPlaceValueBound.IsEmpty() {
		if w.comparer.Compare(w.opts.RequiredInPlaceValueBound.Upper, key.UserKey[:eval.kcmp.PrefixLen]) <= 0 {
			// Common case for CockroachDB. Make it empty since all future keys
			// in this sstable will also have cmpUpper <= 0.
			w.opts.RequiredInPlaceValueBound = UserKeyPrefixBound{}
		} else if w.comparer.Compare(key.UserKey[:eval.kcmp.PrefixLen], w.opts.RequiredInPlaceValueBound.Lower) >= 0 {
			// Don't write to value block if the key is within the bounds.
			return eval, nil
		}
	}
	eval.writeToValueBlock = true
	return eval, nil
}

func (w *ColumnarWriter) drainWriteQueue() {
	defer w.wg.Done()
	for cb := range w.writeQueue {
		if err := w.writable.Write(cb.compressed); err != nil {
			w.err = err
		}
		cb.blockBuf.clear()
		cb.compressed = nil
		compressedBlockPool.Put(cb)
	}
}

func shouldFlushWithoutLatestKV(
	sizeWithKV int,
	sizeWithoutKV int,
	entryCountWithoutKV int,
	flushOptions flushDecisionOptions,
	sizeClassHints []int,
) bool {
	if entryCountWithoutKV == 0 {
		return false
	}
	// For size-class aware flushing we need to account for the metadata that is
	// allocated when this block is loaded into the block cache. For instance, if
	// a block has size 1020B it may fit within a 1024B class. However, when
	// loaded into the block cache we also allocate space for the cache entry
	// metadata. The new allocation of size ~1052B may now only fit within a
	// 2048B class, which increases internal fragmentation.
	sizeWithKV += cache.ValueMetadataSize
	sizeWithoutKV += cache.ValueMetadataSize
	if sizeWithKV < flushOptions.blockSize {
		return false
	}
	if sizeWithoutKV >= flushOptions.blockSize {
		return true
	}

	sizeClassWithKV, withOk := blockSizeClass(sizeWithKV, sizeClassHints)
	sizeClassWithoutKV, withoutOk := blockSizeClass(sizeWithoutKV, sizeClassHints)
	if !withOk || !withoutOk {
		// If the block size could not be mapped to a size class, we fall back
		// to flushing without the KV since we already know sizeWithKV >=
		// blockSize.
		return false
	}
	// Even though sizeWithKV >= blockSize, we may still want to defer flushing
	// if the new size class results in less fragmentation than the block
	// without the KV that does fit within the block size.
	if sizeClassWithKV-sizeWithKV < sizeClassWithoutKV-sizeWithoutKV {
		return false
	}
	return true
}
