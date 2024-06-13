// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/sstable/block"
)

const indexBlockCustomHeaderSize = 8

// IndexBlockWriter writes columnar index blocks. The writer is used for both
// first-level and second-level index blocks, although the resulting
// serialization varies slightly between the two. The index block schema
// consists of three primary columns:
//   - Separators: a user key that is ≥ the largest user key in the corresponding
//     entry.
//   - BlockEndOffset: the offset of the end of the corresponding block.
//   - Lengths: the length, in bytes, of the corresponding block.
//
// The lengths column is always all null for first-level index blocks. Instead,
// iterators infer the start offset of the block by looking at the end offset of
// the previous block. This is possible because data blocks are written
// sequentially without any other interleaved blocks between first-level index
// blocks.
type IndexBlockWriter struct {
	separators      BytesBuilder
	endOffsets      Uint64Builder
	lengths         DefaultNull[*Uint64Builder]
	blockProperties []ColumnWriter
	numColumns      int
	buf             []byte
	rows            int
	// The very first 8 bytes of the index block encode the start offset of the
	// first block. A first-level index block does not have a lengths column and
	// relies on the position of the end offset of the previous block to infer
	// the start offset of the current block. The first row does not have a
	// previous block, so the start offset of the first block is encoded in the
	// custom header. In an sstable with a single first-level index block, this
	// will always be zero.
	firstStartOffset uint64
	alloc            struct {
		lengthsUint64s Uint64Builder
	}
}

const (
	indexBlockColumnSeparator = iota
	indexBlockColumnBlockEndOffset
	indexBlockColumnLengthsOffset
	indexBlockColumnCount
)

// Init initializes the index block writer.
func (w *IndexBlockWriter) Init(blockProperties ...ColumnWriter) {
	w.separators.Init(0)
	w.endOffsets.Init(UintDefaultNone)
	w.alloc.lengthsUint64s.Init(UintDefaultNone)
	w.lengths = MakeDefaultNull(DataTypeUint64, &w.alloc.lengthsUint64s)
	w.numColumns = indexBlockColumnCount
	w.blockProperties = blockProperties
	for i := range w.blockProperties {
		w.numColumns += w.blockProperties[i].NumColumns()
	}
	w.rows = 0
	w.firstStartOffset = 0
}

// Reset resets the index block writer to its initial state, retaining buffers.
func (w *IndexBlockWriter) Reset() {
	w.separators.Reset()
	w.endOffsets.Reset()
	w.lengths.Reset()
	for i := range w.blockProperties {
		w.blockProperties[i].Reset()
	}
	w.rows = 0
	w.firstStartOffset = 0
	w.buf = w.buf[:0]
}

// AddDataBlockHandle adds a new separator and end offset of a data block to the
// index block.  Add returns the index of the row. The caller should add rows to
// the block properties columns too if necessary.
//
// AddDataBlockHandle should only be used for first-level index blocks.
func (w *IndexBlockWriter) AddDataBlockHandle(separator []byte, endOffset, length uint64) int {
	if length > endOffset {
		panic(errors.AssertionFailedf("pebble: length %d > endOffset %d", length, endOffset))
	}
	idx := w.rows
	if idx == 0 {
		w.firstStartOffset = endOffset - length
	} else if prevEndOff := w.endOffsets.elems.At(idx - 1); prevEndOff+block.TrailerLen+length != endOffset {
		panic(errors.AssertionFailedf("pebble: previous block ends at %d; next %d-byte block ends at %d",
			prevEndOff, block.TrailerLen+length, endOffset))
	}
	w.separators.Put(separator)
	w.endOffsets.Set(w.rows, endOffset)
	w.rows++
	return idx
}

// AddIndexBlockHandle adds a new separator and end offset of an index block to
// the second-level index block.  Add returns the index of the row. The caller
// should add rows to the block properties columns too if necessary.
//
// AddIndexBlockHandle should only be used for second-level index blocks.
func (w *IndexBlockWriter) AddIndexBlockHandle(separator []byte, endOffset, length uint64) int {
	idx := w.rows
	w.separators.Put(separator)
	w.endOffsets.Set(idx, endOffset)
	// When building a second-level index block, every entry should have a
	// length. This means the non-NULL index should always match the row index.
	b, j := w.lengths.NotNull(idx)
	if j != idx {
		panic("pebble: missing index block length")
	}
	b.Set(j, length)
	w.rows++
	return idx
}

// Size returns the size of the pending index block.
func (w *IndexBlockWriter) Size() int {
	off := blockHeaderSize(w.numColumns, indexBlockCustomHeaderSize)
	off = w.separators.Size(w.rows, off)
	off = w.endOffsets.Size(w.rows, off)
	off = w.lengths.Size(w.rows, off)
	for i := range w.blockProperties {
		off = w.blockProperties[i].Size(w.rows, off)
	}
	off += 1
	return int(off)
}

// Finish serializes the pending index block.
func (w *IndexBlockWriter) Finish() []byte {
	size := w.Size()
	if cap(w.buf) < size {
		w.buf = make([]byte, size)
	}
	w.buf = w.buf[:size]

	// Write the header, including the index block's custom header encoding the
	// start offset of the first entry's block handle.
	binary.LittleEndian.PutUint64(w.buf, w.firstStartOffset)
	WriteHeader(w.buf[indexBlockCustomHeaderSize:], Header{
		Columns: uint16(w.numColumns),
		Rows:    uint32(w.rows),
	})
	var desc ColumnDesc
	pageOffset := blockHeaderSize(w.numColumns, indexBlockCustomHeaderSize)

	// Write the separators column.
	hOff := blockHeaderSize(indexBlockColumnSeparator, indexBlockCustomHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.separators.Finish(0, w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the end offsets column.
	hOff = blockHeaderSize(indexBlockColumnBlockEndOffset, indexBlockCustomHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.endOffsets.Finish(0, w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the lengths column.
	hOff = blockHeaderSize(indexBlockColumnLengthsOffset, indexBlockCustomHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.lengths.Finish(0, w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the block properties columns. Each block property may encode
	// multiple columns.
	c := indexBlockColumnCount
	for i := range w.blockProperties {
		for j := 0; j < w.blockProperties[i].NumColumns(); j++ {
			hOff = blockHeaderSize(c, indexBlockCustomHeaderSize)
			binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
			pageOffset, desc = w.blockProperties[i].Finish(j, w.rows, pageOffset, w.buf)
			w.buf[hOff] = byte(desc)
			c++
		}
	}

	w.buf[pageOffset] = 0x00
	pageOffset++
	return w.buf
}

// An IndexReader reads columnar index blocks.
type IndexReader struct {
	separators       RawBytes
	endOffsets       UnsafeUint64s
	lengths          UnsafeUint64s // only used for second-level index blocks
	br               BlockReader
	firstStartOffset uint64
	isTwoLevel       bool
}

// Init initializes the index reader with the given serialized index block.
func (r *IndexReader) Init(data []byte) {
	r.firstStartOffset = binary.LittleEndian.Uint64(data[:indexBlockCustomHeaderSize])
	r.br.init(data, indexBlockCustomHeaderSize)
	r.separators = r.br.Column(indexBlockColumnSeparator).RawBytes()
	r.endOffsets = r.br.Column(indexBlockColumnBlockEndOffset).UnsafeUint64s()
	lengths := r.br.Column(indexBlockColumnLengthsOffset)
	r.isTwoLevel = lengths.NullBitmap.Empty() // all non-NULL
	r.lengths = lengths.UnsafeUint64s()
}

// DebugString prints a human-readable explanation of the keyspan block's binary
// representation.
func (r *IndexReader) DebugString() string {
	f := binfmt.New(r.br.data()).LineWidth(20)
	f.HexBytesln(8, "firstStartOffset = %d", r.firstStartOffset)
	r.br.headerToBinFormatter(f)
	for i := 0; i < indexBlockColumnCount; i++ {
		r.br.columnToBinFormatter(f, i, int(r.br.header.Rows))
	}
	return f.String()
}

// IndexIter is an iterator over the block entries in an index block.
type IndexIter struct {
	r   *IndexReader
	n   int
	row int
}

// Init initializes an index iterator from the provided reader.
func (i *IndexIter) Init(r *IndexReader) {
	*i = IndexIter{r: r, n: int(r.br.header.Rows)}
}

// RowIndex returns the index of the block entry at the iterator's current
// position.
func (i *IndexIter) RowIndex() int {
	return i.row
}

// DataBlockHandle returns the data block handle at the iterator's current
// position.
func (i *IndexIter) DataBlockHandle() (bh block.Handle) {
	if invariants.Enabled && i.r.isTwoLevel {
		panic(errors.AssertionFailedf("pebble: DataBlockHandle called on two-level index iterator"))
	}
	if i.row == 0 {
		bh.Offset = i.r.firstStartOffset
	} else {
		bh.Offset = i.r.endOffsets.At(i.row-1) + block.TrailerLen
	}
	bh.Length = i.r.endOffsets.At(i.row) - bh.Offset
	return bh
}

// IndexBlockHandle returns the index block handle at the iterator's current
// position.
func (i *IndexIter) IndexBlockHandle() (bh block.Handle) {
	if invariants.Enabled && !i.r.isTwoLevel {
		panic(errors.AssertionFailedf("pebble: IndexBlockHandle called on first-level index iterator"))
	}
	bh.Offset = i.r.endOffsets.At(i.row) + block.TrailerLen
	bh.Length = i.r.lengths.At(i.row)
	return bh
}

// Separator returns the separator at the iterator's current position. The
// iterator must be positioned at a valid row.
func (i *IndexIter) Separator() []byte {
	return i.r.separators.At(i.row)
}

// SeekGE seeks the index iterator to the block entry with a separator key
// greater or equal to the given key. It returns false if the seek key is
// greater than all index block separators.
func (i *IndexIter) SeekGE(key []byte) bool {
	// Define f(-1) == false and f(upper) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	index, upper := 0, i.n
	for index < upper {
		h := int(uint(index+upper) >> 1) // avoid overflow when computing h
		// index ≤ h < upper

		// TODO(jackson): Is Bytes.At or Bytes.Slice(Bytes.Offset(h),
		// Bytes.Offset(h+1)) faster in this code?
		c := bytes.Compare(key, i.r.separators.At(h))
		if c > 0 {
			index = h + 1 // preserves f(index-1) == false
		} else {
			upper = h // preserves f(upper) == true
		}
	}
	// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true  =>  answer is index.
	i.row = index
	return index < i.n
}

// First seeks index iterator to the first block entry. It returns false if the
// index block is empty.
func (i *IndexIter) First() bool {
	i.row = 0
	return i.n > 0
}

// Last seeks index iterator to the last block entry. It returns false if the
// index block is empty.
func (i *IndexIter) Last() bool {
	i.row = i.n - 1
	return i.n > 0
}

// Next steps the index iterator to the next block entry. It returns false if
// the index block is exhausted.
func (i *IndexIter) Next() bool {
	i.row++
	return i.row < i.n
}

// Prev steps the index iterator to the previous block entry. It returns false
// if the index block is exhausted.
func (i *IndexIter) Prev() bool {
	i.row--
	return i.row >= 0
}
