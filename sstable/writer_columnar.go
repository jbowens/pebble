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
	layout   layoutWriter
	meta     WriterMetadata
	opts     WriterOptions
	// dataBlockOptions and indexBlockOptions are used to configure the sstable
	// block flush heuristics.
	dataBlockOptions     flushDecisionOptions
	indexBlockOptions    flushDecisionOptions
	allocatorSizeClasses []int
	err                  error
	props                Properties
	// block writers buffering unflushed data.
	dataBlock struct {
		colblk.DataBlockWriter
		// size is the result of calling Size() on the data block writer after
		// the last KV was added.
		size int
	}
	indexBlock struct {
		colblk.IndexBlockWriter
		// size is the result of calling Size() on the index block writer after
		// the last block handle was added.
		size int
	}
	rangeDelBlock      colblk.KeyspanBlockWriter
	rangeKeyBlock      colblk.KeyspanBlockWriter
	topLevelIndexBlock colblk.IndexBlockWriter
	metaIndexBlock     colblk.IndexBlockWriter
	valueBlock         *valueBlockWriter
	// filterBlock accumulates the filter block. If populated, the filter
	// ingests either the output of w.split (i.e. a prefix extractor) if w.split
	// is not nil, or the full keys.
	filterBlock filterWriter
	// prevPointKey holds metadata about the previous point key added to the
	// writer. It's updated after each point key is added.
	prevPointKey struct {
		trailer    base.InternalKeyTrailer
		isObsolete bool
	}
	// size is the current size of the completed, compressed data in bytes. As
	// blocks are completed and compressed, size is incremented. Size is updated
	// by the user's foreground goroutine that is building blocks. The goroutine
	// responsible for asynchronously writing to the writable may not have
	// written [size] bytes to the writable yet.
	size uint64
	// writeQueue is a channel for compressed blocks to be written to the
	// underlying writable. As blocks are completed and compressed, they're sent
	// to the writeQueue. A long-running goroutine (see drainWriteQueue) reads
	// from the channel, writing the blocks to the underlying storage.
	writeQueue struct {
		ch  chan *compressedBlock
		wg  sync.WaitGroup
		err error
	}

	separatorBuf          []byte
	tmp                   [blockHandleLikelyMaxLen]byte
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
		layout:   makeLayoutWriter(writable, o),
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
	}
	w.writeQueue.ch = make(chan *compressedBlock)
	w.dataBlock.Init(o.KeySchema)
	w.indexBlock.Init()
	w.topLevelIndexBlock.Init()
	w.metaIndexBlock.Init()
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
	w.writeQueue.wg.Add(1)
	go w.drainWriteQueue()
	return w
}

// Metadata returns the metadata for the finished sstable. Only valid to call
// after the sstable has been finished.
func (w *ColumnarWriter) Metadata() (*WriterMetadata, error) {
	if !w.layout.IsFinished() {
		return nil, errors.New("pebble: writer is not closed")
	}
	return &w.meta, nil
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
	if shouldFlushWithoutLatestKV(size, w.dataBlock.size, entriesWithoutKV, w.dataBlockOptions, w.allocatorSizeClasses) {
		// Flush the data block excluding the key we just added.
		_ = w.flushDataBlock(key.UserKey, eval.kcmp)
		// flushDataBlock reset the data block builder, and we can add the key
		// to this next block now.
		w.dataBlock.Add(key, valuePrefix, valueStoredWithKey, eval.kcmp)
		w.dataBlock.size = w.dataBlock.Size()
	} else {
		// We're not flushing the data block, and we're committing to including
		// the current KV in the block. Remember the new size of the data block
		// with the current KV.
		w.dataBlock.size = size
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

type compressedBlockKind int8

const (
	blockKindInvalid compressedBlockKind = iota
	blockKindData
	blockKindIndex
)

type compressedBlock struct {
	kind       compressedBlockKind
	compressed []byte
	trailer    block.Trailer
	blockBuf   blockBuf
}

func (b *compressedBlock) size() uint64 { return uint64(len(b.compressed)) + block.TrailerLen }

func (w *ColumnarWriter) flushDataBlock(nextKey []byte, kcmp colblk.KeyComparison) (sep []byte) {
	// Compute the separator that will be written to the index block alongside
	// this data block's end offset.
	w.separatorBuf = w.dataBlock.KeyWriter.SeparatePrev(w.separatorBuf[:0], nextKey, kcmp)
	sep = w.separatorBuf

	// Serialize the data block, compress it and send it to the write queue.
	uncompressed := w.dataBlock.Finish()
	w.dataBlock.Reset()
	cb := compressedBlockPool.Get().(*compressedBlock)
	cb.kind = blockKindData
	cb.blockBuf.checksummer.Type = w.opts.Checksum
	cb.compressed, cb.trailer = compressAndChecksum(uncompressed, w.opts.Compression, &cb.blockBuf)
	blockSize := cb.size()
	w.size += blockSize
	w.props.DataSize += blockSize
	w.writeQueue.ch <- cb

	// Add the separator to the index block. This might trigger a flush of the
	// index block too.
	i := w.indexBlock.AddDataBlockHandle(sep, w.size, uint64(len(cb.compressed)))
	sizeWithEntry := w.indexBlock.Size()
	if shouldFlushWithoutLatestKV(sizeWithEntry, w.indexBlock.size, i, w.indexBlockOptions, w.allocatorSizeClasses) {
		w.flushIndexBlock(sep)
	}
	return sep
}

func (w *ColumnarWriter) flushIndexBlock(sep []byte) {
	dataBlockCount := w.indexBlock.Rows()
	uncompressedIndexBlock := w.indexBlock.Finish()
	w.indexBlock.Reset()
	cb := compressedBlockPool.Get().(*compressedBlock)
	cb.kind = blockKindIndex
	cb.blockBuf.checksummer.Type = w.opts.Checksum
	cb.compressed, cb.trailer = compressAndChecksum(uncompressedIndexBlock, w.opts.Compression, &cb.blockBuf)
	blockSize := cb.size()
	w.size += blockSize
	w.props.IndexSize += blockSize
	w.props.NumDataBlocks += uint64(dataBlockCount)

	w.writeQueue.ch <- cb
	h := block.Handle{Offset: w.size, Length: uint64(len(cb.compressed))}
	w.topLevelIndexBlock.AddIndexBlockHandle(sep, h.Offset, h.Length)
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

// Close finishes writing the table and closes the underlying file that the
// table was written to.
func (w *ColumnarWriter) Close() (err error) {
	stopWriteQueue := func() error {
		close(w.writeQueue.ch)
		w.writeQueue.ch = nil
		w.writeQueue.wg.Wait()
		return w.writeQueue.err
	}
	defer func() {
		if w.writeQueue.ch != nil {
			err = errors.CombineErrors(err, stopWriteQueue())
		}
		if w.valueBlock != nil {
			releaseValueBlockWriter(w.valueBlock)
			// Defensive code in case Close gets called again. We don't want to
			// put the same object to a sync.Pool.
			w.valueBlock = nil
		}
		if !w.layout.IsFinished() {
			w.layout.Abort()
		}
	}()
	if w.err != nil {
		return w.err
	}

	// Flush pending blocks.
	// TODO(jackson): Document/enforce invariants here.
	succ := w.flushDataBlock(nil, colblk.KeyComparison{})
	w.flushIndexBlock(succ)

	// Write the top-level index block, but only if we actually wrote more than
	// one index block.
	w.props.IndexType = binarySearchIndex
	if indexPartitions := w.topLevelIndexBlock.Rows(); indexPartitions > 1 {
		uncompressedTopLevelIndexBlock := w.topLevelIndexBlock.Finish()
		w.topLevelIndexBlock.Reset()
		cb := compressedBlockPool.Get().(*compressedBlock)
		cb.blockBuf.checksummer.Type = w.opts.Checksum
		cb.compressed, cb.trailer = compressAndChecksum(uncompressedTopLevelIndexBlock, w.opts.Compression, &cb.blockBuf)
		size := cb.size()

		w.props.IndexType = twoLevelIndex
		w.props.IndexPartitions = uint64(indexPartitions)
		w.props.TopLevelIndexSize = size
		w.props.IndexSize += w.props.TopLevelIndexSize

		w.size += size
		w.writeQueue.ch <- cb
	}

	// Tell the goroutine asynchronously writing to the writable that we're
	// done and then wait for it to finish.
	if err := stopWriteQueue(); err != nil {
		return err
	}
	// Once the write queue has exited, the foreground goroutine takes control
	// of the w.layout layoutWriter.

	// Write the filter block.
	if w.filterBlock != nil {
		w.props.FilterPolicyName = w.filterBlock.policyName()
		bh, err := w.layout.WriteFilterBlock(w.filterBlock)
		if err != nil {
			return err
		}
		w.props.FilterSize = bh.Length
	}

	// Write the range deletion block.
	if w.props.NumRangeDeletions > 0 {
		if _, err := w.layout.WriteRangeDeletionBlock(w.rangeDelBlock.Finish()); err != nil {
			return err
		}
	}

	// Write the range key block.
	if w.props.NumRangeKeys() > 0 {
		if _, err := w.layout.WriteRangeKeyBlock(w.rangeKeyBlock.Finish()); err != nil {
			return err
		}
	}

	// Write the value block index.
	/*
		// TODO(jackson): Deal
		if w.valueBlock != nil {
			vbHandle, vbStats, err := w.valueBlock.finish(w, w.size)
			if err != nil {
				return err
			}
			meta.valueIndex = vbHandle.h
			w.props.NumValueBlocks = vbStats.numValueBlocks
			w.props.NumValuesInValueBlocks = vbStats.numValuesInValueBlocks
			w.props.ValueBlocksSize = vbStats.valueBlocksAndIndexSize
		}
	*/

	w.meta.Size, err = w.layout.Finish()
	if err != nil {
		return err
	}
	w.meta.Properties = w.props
	return nil
}

// drainWriteQueue runs in its own goroutine, writing compressed blocks to the
// underlying writable. It exits when the writeQueue channel is closed. Any
// errors are stored in w.writeQueue.err.
func (w *ColumnarWriter) drainWriteQueue() {
	defer w.writeQueue.wg.Done()
	ch := w.writeQueue.ch
	for cb := range ch {
		// Only write the block if there wasn't an error writing a previous
		// block.
		if w.writeQueue.err == nil {
			switch cb.kind {
			case blockKindData:
				if _, err := w.layout.WritePrecompressedDataBlock(cb.compressed, cb.trailer); err != nil {
					// NB: We can't exit because the foreground goroutine may still
					// offer additional blocks to w.writeQueue.ch.
					w.writeQueue.err = err
				}
			case blockKindIndex:
				if _, err := w.layout.WritePrecompressedIndexBlock(cb.compressed, cb.trailer); err != nil {
					// NB: We can't exit because the foreground goroutine may still
					// offer additional blocks to w.writeQueue.ch.
					w.writeQueue.err = err
				}
			default:
				panic("invalid block")
			}
		}
		cb.blockBuf.clear()
		cb.kind = blockKindInvalid
		cb.compressed = nil
		cb.trailer = block.Trailer{}
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
