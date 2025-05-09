// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/sstable/block"
)

const maxCachedReaders = 5

// A ValueReader is an interface defined over a file that can be used to read
// value blocks.
type ValueReader interface {
	// IndexHandle returns the handle for the file's index block.
	IndexHandle() block.Handle

	// InitReadHandle initializes a ReadHandle for the file, using the provided
	// preallocated read handle to avoid an allocation.
	InitReadHandle(rh *objstorageprovider.PreallocatedReadHandle) objstorage.ReadHandle

	// ReadValueBlock retrieves a value block described by the provided block
	// handle from the block cache, or reads it from the blob file if it's not
	// already cached.
	ReadValueBlock(context.Context, block.ReadEnv, objstorage.ReadHandle,
		block.Handle) (block.BufferHandle, error)

	// ReadIndexBlock retrieves the index block from the block cache, or reads
	// it from the blob file if it's not already cached.
	ReadIndexBlock(context.Context, block.ReadEnv, objstorage.ReadHandle) (block.BufferHandle, error)
}

// A ReaderProvider is an interface that can be used to retrieve a ValueReader
// for a given file number.
type ReaderProvider interface {
	// GetValueReader returns a ValueReader for the given file number.
	GetValueReader(ctx context.Context, fileNum base.DiskFileNum) (r ValueReader, closeFunc func(), err error)
}

// A ValueFetcher retrieves values stored out-of-band in separate blob files.
// The ValueFetcher caches accessed file readers to avoid redundant file cache
// and block cache lookups when performing consecutive value retrievals.
//
// A single ValueFetcher can be used to fetch values from multiple files, and it
// will internally cache readers for each file.
type ValueFetcher struct {
	readerProvider ReaderProvider
	env            block.ReadEnv
	fetchCount     int
	readers        [maxCachedReaders]cachedReader
}

// TODO(jackson): Support setting up a read handle for compaction when relevant.

// Assert that ValueFetcher implements the ValueFetcher interface.
var _ base.ValueFetcher = (*ValueFetcher)(nil)

// Init initializes the ValueFetcher.
func (r *ValueFetcher) Init(rp ReaderProvider, env block.ReadEnv) {
	r.readerProvider = rp
	r.env = env
	if r.readerProvider == nil {
		panic("readerProvider is nil")
	}
}

// Fetch returns the value, given the handle. Fetch must not be called after
// Close.
func (r *ValueFetcher) Fetch(
	ctx context.Context, handle []byte, fileNum base.DiskFileNum, valLen uint32, buf []byte,
) (val []byte, callerOwned bool, err error) {
	handleSuffix := DecodeHandleSuffix(handle)
	vh := Handle{
		FileNum:  fileNum,
		ValueLen: valLen,
		ValueID:  handleSuffix.ValueID,
	}
	v, err := r.retrieve(ctx, vh)
	return v, false, err
}

func (r *ValueFetcher) retrieve(ctx context.Context, vh Handle) (val []byte, err error) {
	// Look for a cached reader for the file. Also, find the least-recently used
	// reader. If we don't find a cached reader, we'll replace the
	// least-recently used reader with the new one for the file indicated by
	// vh.FileNum.
	var cr *cachedReader
	var oldestFetchIndex int
	// TODO(jackson): Reconsider this O(len(readers)) scan.
	for i := range r.readers {
		if r.readers[i].fileNum == vh.FileNum && r.readers[i].r != nil {
			cr = &r.readers[i]
			break
		} else if r.readers[i].lastFetchCount < r.readers[oldestFetchIndex].lastFetchCount {
			oldestFetchIndex = i
		}
	}

	if cr == nil {
		// No cached reader found for the file. Get one from the file cache.
		cr = &r.readers[oldestFetchIndex]
		// Release the previous reader, if any.
		if cr.r != nil {
			if err = cr.Close(); err != nil {
				return nil, err
			}
		}
		if cr.r, cr.closeFunc, err = r.readerProvider.GetValueReader(ctx, vh.FileNum); err != nil {
			return nil, err
		}
		cr.fileNum = vh.FileNum
		cr.rh = cr.r.InitReadHandle(&cr.preallocRH)
	}

	if r.env.Stats != nil {
		r.env.Stats.SeparatedPointValue.ValueBytesFetched += uint64(vh.ValueLen)
	}

	r.fetchCount++
	cr.lastFetchCount = r.fetchCount
	val, err = cr.GetUnsafeValue(ctx, vh, r.env)
	return val, err
}

// Close closes the ValueFetcher and releases all cached readers. Once Close is
// called, the ValueFetcher is no longer usable.
func (r *ValueFetcher) Close() error {
	var err error
	for i := range r.readers {
		if r.readers[i].r != nil {
			err = errors.CombineErrors(err, r.readers[i].Close())
		}
	}
	return err
}

// cachedReader holds a Reader into an open file, and possibly blocks retrieved
// from the block cache.
type cachedReader struct {
	fileNum        base.DiskFileNum
	r              ValueReader
	closeFunc      func()
	rh             objstorage.ReadHandle
	lastFetchCount int
	// indexBlock holds the index block for the file, lazily loaded on the first
	// call to GetUnsafeValue.
	indexBlock struct {
		buf block.BufferHandle
		dec *indexBlockDecoder
	}
	// currentValueBlock holds the currently loaded blob value block, if any.
	currentValueBlock struct {
		// index is the index of the current value block.
		// index is in the range [0, indexBlock.dec.BlockCount()).
		index int
		// loaded indicates whether buf and dec are valid and hold the block
		// identified by index.
		loaded bool
		buf    block.BufferHandle
		dec    *blobValueBlockDecoder
	}
	preallocRH objstorageprovider.PreallocatedReadHandle
}

// GetUnsafeValue retrieves the value for the given handle. The value is
// returned as a byte slice pointing directly into the block cache's data. The
// value is only guaranteed to be stable until the next call to GetUnsafeValue
// or until the cachedReader is closed.
func (cr *cachedReader) GetUnsafeValue(
	ctx context.Context, vh Handle, env block.ReadEnv,
) ([]byte, error) {
	ctx = objiotracing.WithBlockType(ctx, objiotracing.ValueBlock)

	if !cr.indexBlock.buf.Valid() {
		// Read the index block.
		var err error
		cr.indexBlock.buf, err = cr.r.ReadIndexBlock(ctx, env, cr.rh)
		if err != nil {
			return nil, err
		}
		cr.indexBlock.dec = (*indexBlockDecoder)(unsafe.Pointer(cr.indexBlock.buf.BlockMetadata()))
	}

	// Determine which block contains the value.
	//
	// If we already have a block loaded (eg, we're scanning retrieving multiple
	// values), the current block might contain the value.
	if !cr.currentValueBlock.loaded || cr.currentValueBlock.dec.minimumValueID > vh.ValueID ||
		vh.ValueID > ValueID(cr.indexBlock.dec.maxValueIDs.At(cr.currentValueBlock.index)) {
		// Otherwise, seek within the index block's maxValueIDs column to find
		// which block contains the value.
		blockIndex := cr.indexBlock.dec.Seek(vh.ValueID)
		if invariants.Enabled {
			if blockIndex >= cr.indexBlock.dec.BlockCount() {
				return nil, errors.AssertionFailedf("value ID out of range of blob file: %d > %d",
					vh.ValueID, cr.indexBlock.dec.maxValueIDs.At(cr.indexBlock.dec.BlockCount()-1))
			}
			if maxValueID := ValueID(cr.indexBlock.dec.maxValueIDs.At(blockIndex)); vh.ValueID > maxValueID {
				return nil, errors.AssertionFailedf("value ID out of range: %d > %d", vh.ValueID, maxValueID)
			}
		}

		// Retreive the block's handle and read the blob value block.
		h := cr.indexBlock.dec.BlockHandle(blockIndex)
		cr.currentValueBlock.buf.Release()
		cr.currentValueBlock.loaded = false
		var err error
		cr.currentValueBlock.buf, err = cr.r.ReadValueBlock(ctx, env, cr.rh, h)
		if err != nil {
			return nil, err
		}
		cr.currentValueBlock.dec = (*blobValueBlockDecoder)(unsafe.Pointer(cr.currentValueBlock.buf.BlockMetadata()))
		cr.currentValueBlock.index = blockIndex
		cr.currentValueBlock.loaded = true
		if invariants.Enabled {
			maxValueID := cr.currentValueBlock.dec.minimumValueID + ValueID(cr.currentValueBlock.dec.bd.Rows())
			if vh.ValueID > maxValueID {
				panic(errors.AssertionFailedf("value ID out of range: %d > %d", vh.ValueID, maxValueID))
			}
		}
	}
	v := cr.currentValueBlock.dec.Value(vh.ValueID)
	if invariants.Enabled && len(v) != int(vh.ValueLen) {
		panic(errors.AssertionFailedf("value length mismatch: %d != %d", len(v), vh.ValueLen))
	}
	return v, nil
}

// Close releases resources associated with the reader.
func (cfr *cachedReader) Close() (err error) {
	if cfr.rh != nil {
		err = cfr.rh.Close()
	}
	cfr.indexBlock.buf.Release()
	cfr.currentValueBlock.buf.Release()
	// Release the cfg.Reader. closeFunc is provided by the file cache and
	// decrements the refcount on the open file reader.
	cfr.closeFunc()
	*cfr = cachedReader{}
	return err
}
