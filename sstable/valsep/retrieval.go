// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valsep

import (
	"context"
	"runtime/debug"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

const maxCachedReaders = 5

type ValueReader interface {
	ValueIndexHandle() valblk.IndexHandle

	// InitReadHandle
	InitReadHandle(rh *objstorageprovider.PreallocatedReadHandle) objstorage.ReadHandle

	// ReadValueBlock retrieves a value block described by the provided block
	// handle.
	ReadValueBlock(context.Context, block.ReadEnv, objstorage.ReadHandle,
		block.Handle) (block.BufferHandle, error)

	// ReadValueIndexBlock retrieves the index block from the block cache, or
	// reads it from the blob file if it's not already cached.
	ReadValueIndexBlock(context.Context, block.ReadEnv, objstorage.ReadHandle) (block.BufferHandle, error)
}

type ReaderProvider interface {
	GetValueReader(ctx context.Context, it any, fileNum base.DiskFileNum) (r ValueReader, closeFunc func(any), err error)
}

var retrieverPool = sync.Pool{
	New: func() interface{} {
		return &PooledValueRetriever{}
	},
}

// NewRetriever creates a new, pooled Retriever. Closing the returned retriever
// returns it to the pool.
func NewRetriever(rp ReaderProvider, env block.ReadEnv) *PooledValueRetriever {
	if rp == nil {
		panic("readerProvider is nil" + string(debug.Stack()))
	}
	r := retrieverPool.Get().(*PooledValueRetriever)
	(*Retriever)(r).Init(rp, env)
	return r
}

type PooledValueRetriever Retriever

func (pvr *PooledValueRetriever) DecodeAttributes(handle []byte) base.ValueAttributes {
	r := (*Retriever)(pvr)
	return r.DecodeAttributes(handle)
}

func (pvr *PooledValueRetriever) Retrieve(ctx context.Context, file base.DiskFileNum, handle []byte) (val []byte, err error) {
	r := (*Retriever)(pvr)
	return r.Retrieve(ctx, file, handle)
}

func (pvr *PooledValueRetriever) Close() error {
	r := (*Retriever)(pvr)
	err := r.Close()
	*pvr = PooledValueRetriever{}
	retrieverPool.Put(pvr)
	return err
}

func (pvr *PooledValueRetriever) CloseHook(any) {
	// TODO(jackson): error return value
	_ = pvr.Close()
}

// A Retriever retrieves values stored out-of-band in a separate blob file. When
// performing multiple consecutive value retrievals, the Iterator caches
// accessed file readers to avoid redundant file cache lookups, block cache
// lookups and to preserve read handles.
type Retriever struct {
	readerProvider ReaderProvider
	env            block.ReadEnv
	fetchCount     int
	readers        [maxCachedReaders]cachedReader
}

// Tktk setup readhandle for compaction when applicable.

// Assert that Retriever implements the ValueRetriever interface.
var _ base.ValueRetriever = (*Retriever)(nil)

func (r *Retriever) Init(rp ReaderProvider, env block.ReadEnv) {
	r.readerProvider = rp
	r.env = env
	if r.readerProvider == nil {
		panic("readerProvider is nil")
	}
}

func (r *Retriever) DecodeAttributes(handle []byte) base.ValueAttributes {
	var attrs base.ValueAttributes
	attrs.ShortAttribute = block.ValuePrefix(handle[0]).ShortAttribute()
	attrs.ShortAttributeExists = true
	len, _ := valblk.DecodeLenFromHandle(handle[1:])
	attrs.ValueLen = int(len)
	return attrs
}

func (r *Retriever) Retrieve(ctx context.Context, fileNum base.DiskFileNum, handle []byte) (val []byte, err error) {

	if r == nil {
		panic("retriever is nil")
	}

	// Trim off the ValuePrefix byte.
	vh := valblk.DecodeHandle(handle[1:])

	// Look for a cached reader for the file. Also, find the least-recently used
	// reader. If we don't find a cached reader, we'll replace the
	// least-recently used reader with the new one for the file indicated by
	// fileNum.
	var cr *cachedReader
	var oldestFetchIndex int
	for i := range r.readers {
		if r.readers[i].fileNum == fileNum && r.readers[i].r != nil {
			cr = &r.readers[i]
			break
		} else if r.readers[i].lastFetchCount < r.readers[oldestFetchIndex].lastFetchCount {
			oldestFetchIndex = i
		}
	}

	if r.readerProvider == nil {
		panic("readerProvider is nil")
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
		if cr.r, cr.closeFunc, err = r.readerProvider.GetValueReader(ctx, cr, fileNum); err != nil {
			return nil, err
		}
		cr.fileNum = fileNum
		cr.rh = cr.r.InitReadHandle(&cr.preallocRH)
	}

	r.fetchCount++
	cr.lastFetchCount = r.fetchCount
	val, err = cr.GetUnsafeValue(ctx, vh, r.env)
	return val, err
}

func (r *Retriever) Close() error {
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
	fileNum            base.DiskFileNum
	r                  ValueReader
	closeFunc          func(any)
	rh                 objstorage.ReadHandle
	lastFetchCount     int
	currentBlockNum    uint32
	currentBlockLoaded bool
	currentBlockBuf    block.BufferHandle
	indexBlockBuf      block.BufferHandle
	preallocRH         objstorageprovider.PreallocatedReadHandle
}

// GetUnsafeValue retrieves the value for the given handle. The value is
// returned as a byte slice pointing directly into the block cache's data. The
// value is only guaranteed to be stable until the next call to GetUnsafeValue
// or until the cachedFileReader is closed.
func (cr *cachedReader) GetUnsafeValue(ctx context.Context, vh base.ValueHandle, env block.ReadEnv) ([]byte, error) {
	ctx = objiotracing.WithBlockType(ctx, objiotracing.ValueBlock)

	if !cr.indexBlockBuf.Valid() {
		// Read the index block.
		var err error
		cr.indexBlockBuf, err = cr.r.ReadValueIndexBlock(ctx, env, cr.rh)
		if err != nil {
			return nil, err
		}
	}

	if !cr.currentBlockLoaded || vh.BlockNum != cr.currentBlockNum {
		// Translate the handle's block number into a block handle via the blob
		// file's index block.
		h, err := valblk.DecodeBlockHandleFromIndex(cr.indexBlockBuf.BlockData(), vh.BlockNum, cr.r.ValueIndexHandle())
		if err != nil {
			return nil, err
		}
		cr.currentBlockBuf.Release()
		cr.currentBlockLoaded = false
		cr.currentBlockBuf, err = cr.r.ReadValueBlock(ctx, env, cr.rh, h)
		if err != nil {
			return nil, err
		}
		cr.currentBlockNum = vh.BlockNum
		cr.currentBlockLoaded = true
	}
	data := cr.currentBlockBuf.BlockData()
	if len(data) < int(vh.OffsetInBlock+vh.ValueLen) {
		return nil, base.CorruptionErrorf("blob file %s: block %d: expected block length %d, got %d",
			vh.FileNum, vh.BlockNum, vh.OffsetInBlock+vh.ValueLen, len(data))
	}
	return data[vh.OffsetInBlock : vh.OffsetInBlock+vh.ValueLen], nil
}

// Close releases resources associated with the reader.
func (cfr *cachedReader) Close() (err error) {
	if cfr.rh != nil {
		err = cfr.rh.Close()
	}
	cfr.indexBlockBuf.Release()
	cfr.currentBlockBuf.Release()
	// Release the cfg.Reader. closeFunc is provided by the file cache and
	// decrements the refcount on the open file reader.
	cfr.closeFunc(cfr)
	*cfr = cachedReader{}
	return err
}
