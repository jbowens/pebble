// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package blob implements mechanics for encoding and decoding values into blob
// files.
//
// # Blob file format
//
// A blob file consists of a sequence of a sequence of blob-value blocks
// containing values, followed by an index block describing the location of the
// blob-value blocks. At the tail of the file is a fixed-size footer encoding
// the exact offset and length of the index block.
//
// Semantically, a blob file implements an array of blob values. The index into
// a blob file is a ValueID. A reader retrieving a particular value must use the
// index block to identify which blob-value block contains the value, load the
// blob-value block and use its structure to retrieve the value.
//
// ## Index Block
//
// The index block is used to determine which blob-value block contains a
// particular value and the block's location within the file. The index block
// uses a columnar encoding (see pkg colblk) to encode two columns:
//
// **Offsets**:
// an array of uints encoding the offset in the blob file at which
// each block begins. There is +1 offset than there are blocks. The last offset
// points to the first byte after the last block. The length of blocks is
// inferred through the difference between consecutive offsets.
//
// **MaxValueIDs**:
// an array of uints encoding the maximum value ID in each block.
// Elements in this array are strictly increasing (a value may only exist in 1
// block).
//
// A reader binary searches the MaxValueIDs column to find which block must
// contain a value for a given ValueID [see indexBlockDecoder.Seek]. It then
// reads the block's offset and infers the block's length form the consecutive
// block's offset [see indexBlockDecoder.BlockHandle].
//
// ## Blob Value Blocks
//
// A blob value block is a columnar block encoding blob values. It encodes a
// single column: a RawBytes of values. The colblk.RawBytes encoding allows
// constant-time access to the i'th value within the block. The block has a
// 4-byte custom header encoding the minimum value ID encoded within the block.
// A reader must subtract this minimum value ID from their sought value ID to
// determine the index of the value within the colblk.RawBytes column.
//
// Note that the minimum value ID is stored within the value block header and
// not as a column in the index block because it is only needed when retrieving
// a value from within the block. A value fetcher always knows that the value
// identified by their value ID must exist in the blob file, so there is no
// opportunity to avoid block loads. Storing the minimum value ID outside the
// index block reduces the index block's size, improving cache locality.
//
// ## Value ID Sparseness
//
// A blob file is allowed to be sparse, meaning that some value IDs may not have
// a corresponding blob value. This sparseness exists within the gaps between
// blocks. The i'th block may encode a maximum value ID of N in the index block,
// and the i+1'th block may encoded a minimum value ID of M. If M > N+1, then
// there exists a gap between the value IDs.
//
// Sparseness allows blob file rewrites to rewrite a blob file and avoid
// encoding values that are no longer referenced. If a value between the min and
// max value IDs of a block is no longer referenced, value ID is represented as
// an empty byte slice within the RawBytes column. This requires one offset (1-4
// bytes) to represent. This is considered tolerable, because we expect
// significant locality to gaps in referenced values. Compactions will remove
// swaths of references all at once, typically all the values of keys that fall
// within a narrow keyspan. This locality allows us to represent the sparseness
// using the gaps between blocks.
//
// If we find this to not hold for some reason, we can extend the blob-value
// block format to encode a NullBitmap. This would allow us to represent missing
// values using 2-bits per value ID.
//
// ## Diagram
//
// +------------------------------------------------------------------------------+
// |                             BLOB FILE FORMAT                                 |
// +------------------------------------------------------------------------------+
// |                              Value Block #0                                  |
// |   +----------------------------------------------------------------------+   |
// |   | Min Value ID: 0                                                      |   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                              Value Block #1                                  |
// |   +----------------------------------------------------------------------+   |
// |   | Min Value ID: 100                                                    |   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                                 ...                                          |
// |                              Value Block #N                                  |
// |   +----------------------------------------------------------------------+   |
// |   | Min Value ID: 900                                                    |   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                                                                              |
// +------------------------------- Index Block ----------------------------------+
// |   +-------------------------------+    +---------------------------------+   |
// |   | MaxValueIDs (N):              |    | Offsets (N+1):                  |   |
// |   | [99, 899, 1945, ...]          |    | [0, 1200, 2506, ...]            |   |
// |   +-------------------------------+    +---------------------------------+   |
// +----------------------------- Footer (30 bytes) ------------------------------+
// | CRC Checksum (4 bytes)                                                       |
// | Index Block Offset (8 bytes)                                                 |
// | Index Block Length (8 bytes)                                                 |
// | Checksum Type (1 byte)                                                       |
// | Format (1 byte)                                                              |
// | Magic String (8 bytes)                                                       |
// +------------------------------------------------------------------------------+
package blob

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const (
	indexBlockColumnCount = 2

	indexBlockColumnMaxValueIDsIdx = 0
	indexBlockColumnOffsetsIdx     = 1
)

// indexBlockEncoder encodes a blob index block.
//
// A blob index block is a columnar block containing a single column: an array
// of uints encoding the file offset at which each block begins. The last entry
// in the array points after the last block.
type indexBlockEncoder struct {
	// countBlocks is the number of blocks in the index block. The number of
	// rows in the offsets column is countBlocks+1. The last offset points to
	// the first byte after the last block so that a reader can compute the
	// length of the last block.
	countBlocks int
	// maxValueIDs contains the maximum value ID of a value contained in each
	// block. Values in this column are strictly increasing.
	maxValueIDs colblk.UintBuilder
	// offsets contains the offset of the start of each block. There is +1 more
	// offset than there are blocks, with the last offset pointing to the first
	// byte after the last block. Block lengths are inferred from the difference
	// between consecutive offsets.
	offsets colblk.UintBuilder
	enc     colblk.BlockEncoder
}

// Init initializes the index block encoder.
func (e *indexBlockEncoder) Init() {
	e.maxValueIDs.Init()
	e.offsets.Init()
	e.countBlocks = 0
}

// Reset resets the index block encoder to its initial state, retaining buffers.
func (e *indexBlockEncoder) Reset() {
	e.maxValueIDs.Reset()
	e.offsets.Reset()
	e.countBlocks = 0
	e.enc.Reset()
}

// AddBlockHandle adds a handle to a blob-value block to the index block. The
// maxValueID must be the maximum value ID of a value contained in the block.
func (e *indexBlockEncoder) AddBlockHandle(h block.Handle, maxValueID ValueID) {
	// Every call to AddBlockHandle adds its end offset (i.e, the next block's
	// start offset) to the offsets column.
	//
	// The first call to AddBlockHandle must also add the start offset the first
	// block. We also verify that for subsequent blocks, the start offset
	// matches the offset encoded by the previous call to AddBlockHandle.
	if e.countBlocks == 0 {
		e.offsets.Set(0, h.Offset)
	} else if expected := e.offsets.Get(e.countBlocks); expected != h.Offset {
		panic(errors.AssertionFailedf("block handle %s doesn't have expected offset of %d", h, expected))
	}

	// Set the value ID separator (the maximum value ID in the block) for the
	// block.
	e.maxValueIDs.Set(e.countBlocks, uint64(maxValueID))

	// Increment the number blocks, and set the endOffset.
	e.countBlocks++
	endOffset := h.Offset + h.Length + block.TrailerLen
	e.offsets.Set(e.countBlocks, endOffset)
}

func (e *indexBlockEncoder) size() int {
	off := colblk.HeaderSize(indexBlockColumnCount, 0 /* custom header size */)
	if e.countBlocks > 0 {
		off = e.maxValueIDs.Size(e.countBlocks, off)
		off = e.offsets.Size(e.countBlocks+1, off)
	}
	off++
	return int(off)
}

// Finish serializes the pending index block.
func (e *indexBlockEncoder) Finish() []byte {
	if invariants.Enabled {
		// Validate that the maximum value IDs are strictly increasing.
		for i := 1; i < e.countBlocks; i++ {
			if e.maxValueIDs.Get(i-1) >= e.maxValueIDs.Get(i) {
				panic(errors.AssertionFailedf("value ID separators must be strictly increasing; %d (block %d) >= %d (block %d)",
					e.maxValueIDs.Get(i-1), i-1, e.maxValueIDs.Get(i), i))
			}
		}
	}

	e.enc.Init(e.size(), colblk.Header{
		Version: colblk.Version1,
		Columns: indexBlockColumnCount,
		Rows:    uint32(e.countBlocks),
	}, 0 /* custom header size */)
	e.enc.Encode(e.countBlocks, &e.maxValueIDs)
	e.enc.Encode(e.countBlocks+1, &e.offsets)
	return e.enc.Finish()
}

// An indexBlockDecoder decodes blob index blocks.
type indexBlockDecoder struct {
	// offsets contains the offset of the start of each block. There is +1 more
	// offset than there are blocks, with the last offset pointing to the first
	// byte after the last block. Block lengths are inferred from the difference
	// between consecutive offsets.
	offsets colblk.UnsafeUints
	// maxValueIDs contains the maximum value ID of a value contained in each
	// block. Values in this column are strictly increasing.
	maxValueIDs colblk.UnsafeUints
	bd          colblk.BlockDecoder
}

// Init initializes the index block decoder with the given serialized index
// block.
func (r *indexBlockDecoder) Init(data []byte) {
	r.bd.Init(data, 0 /* custom header size */)
	r.maxValueIDs = colblk.DecodeColumn(&r.bd, indexBlockColumnMaxValueIDsIdx,
		r.bd.Rows(), colblk.DataTypeUint, colblk.DecodeUnsafeUints)
	// Decode the offsets column. We pass rows+1 because an index block encoding
	// n block handles encodes n+1 offsets.
	r.offsets = colblk.DecodeColumn(&r.bd, indexBlockColumnOffsetsIdx,
		r.bd.Rows()+1, colblk.DataTypeUint, colblk.DecodeUnsafeUints)
}

// BlockHandle returns the block handle for the given block index in the
// range [0, bd.Rows()).
func (r *indexBlockDecoder) BlockHandle(blockIndex int) block.Handle {
	invariants.CheckBounds(int(blockIndex), r.bd.Rows())
	// TODO(jackson): Add an At2 method to the UnsafeUints type too.
	offset := r.offsets.At(int(blockIndex))
	offset2 := r.offsets.At(int(blockIndex) + 1)
	return block.Handle{
		Offset: offset,
		Length: offset2 - offset - block.TrailerLen,
	}
}

// BlockCount returns the number of blocks in the index block.
func (r *indexBlockDecoder) BlockCount() int {
	return int(r.bd.Rows())
}

// Seek binary searches the index block's maxValueIDs column to find the block
// that must contain a value for the given value ID. It returns the index of the
// block that must contain the value.
func (r *indexBlockDecoder) Seek(id ValueID) int {
	target := uint64(id)
	n := r.bd.Rows()
	// Define x[k] = r.maxValueIDs.At(k).
	// Define x[-1] < target and x[n] >= target.
	// Invariant: x[i-1] < target, x[j] >= target.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		if r.maxValueIDs.At(h) < target {
			i = h + 1 // preserves x[i-1] < target
		} else {
			j = h // preserves x[j] >= target
		}
	}
	// i == j
	if invariants.Enabled && i > 0 && r.maxValueIDs.At(i-1) >= target {
		panic(errors.AssertionFailedf("seek ID %d is less than %d; should've returned blk%d instead of blk%d (max value ID %d)",
			id, r.maxValueIDs.At(i-1), i-1, i, r.maxValueIDs.At(i-1)))
	}
	return i
}

// DebugString prints a human-readable explanation of the index block's binary
// representation.
func (r *indexBlockDecoder) DebugString() string {
	f := binfmt.New(r.bd.Data()).LineWidth(20)
	tp := treeprinter.New()
	r.Describe(f, tp.Child("index-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the index block, assuming f.Offset()
// is positioned at the beginning of the same index block described by r.
func (r *indexBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("index block header")
	r.bd.HeaderToBinFormatter(f, n)
	r.bd.ColumnToBinFormatter(f, n, indexBlockColumnMaxValueIDsIdx, r.bd.Rows())
	r.bd.ColumnToBinFormatter(f, n, indexBlockColumnOffsetsIdx, r.bd.Rows()+1)
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// Assert that an IndexBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(indexBlockDecoder{}))

// initIndexBlockMetadata initializes the index block metadata.
func initIndexBlockMetadata(md *block.Metadata, data []byte) (err error) {
	if uintptr(unsafe.Pointer(md))%8 != 0 {
		return errors.AssertionFailedf("metadata is not 8-byte aligned")
	}
	d := (*indexBlockDecoder)(unsafe.Pointer(md))
	// Initialization can panic; convert panics to corruption errors (so higher
	// layers can add file number and offset information).
	defer func() {
		if r := recover(); r != nil {
			err = base.CorruptionErrorf("error initializing index block metadata: %v", r)
		}
	}()
	d.Init(data)
	return nil
}

const (
	blobValueBlockCustomHeaderSize = 4
	blobValueBlockColumnCount      = 1
	blobValueBlockColumnValuesIdx  = 0
)

// blobValueBlockEncoder encodes a blob value block.
//
// A blob value block is a columnar block containing a single column: an array
// of bytes encoding values.
type blobValueBlockEncoder struct {
	values colblk.RawBytesBuilder
	enc    colblk.BlockEncoder
	minID  ValueID
}

// Init initializes the blob value block encoder. minID must be the value ID of
// the first value that will be added to the block.
func (e *blobValueBlockEncoder) Init(minID ValueID) {
	e.values.Init()
	e.minID = minID
}

// Reset resets the blob value block encoder to its initial state, retaining
// buffers. minID must be the value ID of the first value that will be added to
// the block.
func (e *blobValueBlockEncoder) Reset(minID ValueID) {
	e.values.Reset()
	e.enc.Reset()
	e.minID = minID
}

// AddValue adds a value to the blob value block. The value appended will be
// retrievable by the value ID formed by e.minimumValueID + e.values.Rows().
func (e *blobValueBlockEncoder) AddValue(v []byte) {
	e.values.Put(v)
}

// Count returns the number of values in the blob value block.
func (e *blobValueBlockEncoder) Count() int {
	return e.values.Rows()
}

func (e *blobValueBlockEncoder) size() int {
	off := colblk.HeaderSize(blobValueBlockColumnCount, blobValueBlockCustomHeaderSize)
	if rows := e.values.Rows(); rows > 0 {
		off = e.values.Size(rows, off)
	}
	off++
	return int(off)
}

// Finish serializes the pending blob value block.
func (e *blobValueBlockEncoder) Finish() []byte {
	e.enc.Init(e.size(), colblk.Header{
		Version: colblk.Version1,
		Columns: blobValueBlockColumnCount,
		Rows:    uint32(e.values.Rows()),
	}, blobValueBlockCustomHeaderSize)
	e.enc.Encode(e.values.Rows(), &e.values)
	data := e.enc.Finish()
	binary.LittleEndian.PutUint32(data, uint32(e.minID))
	return data
}

// A blobValueBlockDecoder reads columnar blob value blocks.
type blobValueBlockDecoder struct {
	minimumValueID ValueID
	values         colblk.RawBytes
	bd             colblk.BlockDecoder
}

// Init initializes the decoder with the given serialized blob value block.
func (d *blobValueBlockDecoder) Init(data []byte) {
	d.minimumValueID = ValueID(binary.LittleEndian.Uint32(data))
	d.bd.Init(data, blobValueBlockCustomHeaderSize)
	d.values = d.bd.RawBytes(blobValueBlockColumnValuesIdx)
}

// Value returns the value for the given value ID. It panics if the value ID is
// out of range this block's value IDs.
func (d *blobValueBlockDecoder) Value(id ValueID) []byte {
	if id < d.minimumValueID || (id-d.minimumValueID) >= ValueID(d.bd.Rows()) {
		panic(errors.AssertionFailedf("value ID %d is out of range: block min=%d, rows=%d", id, d.minimumValueID, d.bd.Rows()))
	}
	return d.values.At(int(id - d.minimumValueID))
}

// DebugString prints a human-readable explanation of the blob value block's
// binary representation.
func (d *blobValueBlockDecoder) DebugString() string {
	f := binfmt.New(d.bd.Data()).LineWidth(20)
	tp := treeprinter.New()
	d.Describe(f, tp.Child("blob-value-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the blob value block, assuming
// f.Offset() is positioned at the beginning of the same blob value block
// described by r.
func (d *blobValueBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("blob value block header")
	f.HexBytesln(4, "minimum value ID: %d", d.minimumValueID)
	d.bd.HeaderToBinFormatter(f, n)
	d.bd.ColumnToBinFormatter(f, n, blobValueBlockColumnValuesIdx, d.bd.Rows())
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// Assert that an BlobBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(blobValueBlockDecoder{}))

// initBlobValueBlockMetadata initializes the blob value block metadata.
func initBlobValueBlockMetadata(md *block.Metadata, data []byte) (err error) {
	if uintptr(unsafe.Pointer(md))%8 != 0 {
		return errors.AssertionFailedf("metadata is not 8-byte aligned")
	}
	d := (*blobValueBlockDecoder)(unsafe.Pointer(md))
	// Initialization can panic; convert panics to corruption errors (so higher
	// layers can add file number and offset information).
	defer func() {
		if r := recover(); r != nil {
			err = base.CorruptionErrorf("error initializing blob value block metadata: %v", r)
		}
	}()
	d.Init(data)
	return nil
}
