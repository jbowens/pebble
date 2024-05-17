// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"golang.org/x/exp/constraints"
)

// Version indicates the version of the columnar block format encoded.
type Version uint8

// Header holds the metadata extracted from a columnar block's header.
type Header struct {
	// Columns holds the number of columns encoded within the block.
	Columns uint16
	// Rows holds the number of rows encoded within the block.
	Rows uint32
}

func (h Header) String() string {
	return fmt.Sprintf("Columns=%d; Rows=%d", h.Columns, h.Rows)
}

func blockHeaderSize(cols int, customHeaderSize int) uint32 {
	// Each column has a 1-byte ColumnDesc and a 4-byte offset into the block.
	return uint32(6 + cols*5 + customHeaderSize)
}

// ReadHeader reads the block header from the provided serialized columnar block.
func ReadHeader(data []byte) Header {
	return Header{
		Columns: uint16(binary.LittleEndian.Uint16(data)),
		Rows:    uint32(binary.LittleEndian.Uint32(data[align16:])),
	}
}

var emptyBytesData = make([]byte, 0)

// ColumnWriter TODO(peter) ...
type ColumnWriter interface {
	PutBitmap(col int, v bool)
	PutUint8(col int, v uint8)
	PutUint16(col int, v uint16)
	PutUint32(col int, v uint32)
	PutUint64(col int, v uint64)
	PutRawBytes(col int, v []byte)
	PutPrefixBytes(col int, v []byte) bool
	PutNull(col int)
}

// ColumnConfig specifies how a column may be encoded.
type ColumnConfig struct {
	DataType DataType
	// BundleSize is only used for DataTypePrefixBytes columns. // TODO(jackson):
	BundleSize int
}

// String returns a human-readable string representation of the column
// configuration.
func (c ColumnConfig) String() string {
	s := c.DataType.String()
	if c.DataType == DataTypePrefixBytes && c.BundleSize > 0 {
		s += fmt.Sprintf("(%d)", c.BundleSize)
	}
	return s
}

type columnBuilder struct {
	config    ColumnConfig
	data      []byte
	bytes     bytesBuilder
	nulls     nullBitmapBuilder
	bitmap    bitmapBuilder
	count     uint32
	nullCount uint32
	minInt    uint64
	maxInt    uint64
}

func (b *columnBuilder) reset() {
	b.data = b.data[:0]
	switch b.config.DataType {
	case DataTypeBool:
		// Zero the bitmap so that we can reuse it without having to explicitly
		// clear each false bit.
		for i := range b.bitmap {
			b.bitmap[i] = 0
		}
		b.bitmap = b.bitmap[:0]
	case DataTypeBytes:
		b.bytes.Reset(0)
	case DataTypePrefixBytes:
		b.bytes.Reset(b.config.BundleSize)
	}
	// Zero the null bitmap so that we can reuse it without having to explicitly
	// clear each non-null bit.
	for i := range b.nulls {
		b.nulls[i] = 0
	}
	b.nulls = b.nulls[:0]
	b.count = 0
	b.nullCount = 0
	b.minInt = math.MaxUint64
	b.maxInt = 0
}

// grow reserves n bytes in the column builder's buffer, growing the buffer if
// necessary. It returns a byte slice into which the caller may write n bytes.
func (b *columnBuilder) grow(n int) []byte {
	i := len(b.data)
	if cap(b.data)-i < n {
		// Double the size of the buffer, or initialize it to at least 256
		// bytes if this is the first allocation. Then double until there's
		// sufficient space for n bytes.
		newSize := max(cap(b.data)<<1, 256)
		for newSize-i < n {
			newSize <<= 1 /* double the size */
		}
		newData := make([]byte, i, newSize)
		copy(newData, b.data)
		b.data = newData
	}
	b.data = b.data[:i+n]
	return b.data[i:]
}

// TODO(jackson): Add non-invariants gated runtime assertions on data types, but
// performed once per-block. We can refactor the interface so the caller creates
// a Uint64Writer, at which point the column type is asserted.

func (b *columnBuilder) putBitmap(v bool) {
	if invariants.Enabled && b.config.DataType != DataTypeBool {
		panic("bool column value expected")
	}
	b.bitmap = b.bitmap.Set(int(b.count), v)
	b.count++
}

func (b *columnBuilder) putUint8(v uint8) {
	if invariants.Enabled && b.config.DataType != DataTypeUint8 {
		panic("fixed8 column value expected")
	}
	b.data = append(b.data, byte(v))
	b.count++
}

func (b *columnBuilder) putUint16(v uint16) {
	if invariants.Enabled && b.config.DataType != DataTypeUint16 {
		panic("fixed16 column value expected")
	}
	binary.LittleEndian.PutUint16(b.grow(2), v)
	b.count++
}

func (b *columnBuilder) putUint32(v uint32) {
	if invariants.Enabled && b.config.DataType != DataTypeUint32 {
		panic("fixed32 column value expected")
	}
	if uint64(v) < b.minInt {
		b.minInt = uint64(v)
	}
	if uint64(v) > b.maxInt {
		b.maxInt = uint64(v)
	}
	binary.LittleEndian.PutUint32(b.grow(4), v)
	b.count++
}

func (b *columnBuilder) putUint64(v uint64) {
	if invariants.Enabled && b.config.DataType != DataTypeUint64 {
		panic("fixed64 column value expected")
	}
	if v < b.minInt {
		b.minInt = v
	}
	if v > b.maxInt {
		b.maxInt = v
	}
	binary.LittleEndian.PutUint64(b.grow(8), v)
	b.count++
}

func (b *columnBuilder) putRawBytes(v []byte) {
	if invariants.Enabled && b.config.DataType != DataTypeBytes {
		panicf("bytes column value expected; have %s", b.config.DataType)
	}
	b.bytes.Put(v)
	b.count++
}

func (b *columnBuilder) putRawBytesConcat(v1, v2 []byte) {
	if invariants.Enabled && b.config.DataType != DataTypeBytes {
		panicf("bytes column value expected; have %s", b.config.DataType)
	}
	b.bytes.PutConcat(v1, v2)
	b.count++
}

func (b *columnBuilder) putPrefixBytes(v []byte) (samePrefix bool) {
	if invariants.Enabled && b.config.DataType != DataTypeBytes {
		panicf("bytes column value expected; have %s", b.config.DataType)
	}
	b.count++
	return b.bytes.PutOrdered(v)
}

func (b *columnBuilder) putNull() {
	if invariants.Enabled {
		switch b.config.DataType {
		case DataTypeBool:
			// Disable the NULL-bitmap for the bool type where it doesn't make
			// sense because the 2-bits/row of the NULL-bitmap is larger than the
			// 1-bit/row of the Bitmap. If you want to be able to mark a boolean
			// column values as NULL, use another Bitmap column instead.
			panicf("NULL values unsupported for %s columns", b.config.DataType)
		}
	}
	b.nulls = b.nulls.Set(int(b.count), true)
	b.count++
	b.nullCount++
}

func (w *columnBuilder) encode(offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(w.config.DataType)

	if w.nullCount == w.count {
		desc = desc.WithEncoding(EncodingAllNull)
		return offset, desc
	}
	if w.nullCount > 0 {
		// The NULL-bitmap.
		desc = desc.WithNullBitmap()
		offset = w.nulls.Finish(offset, buf)
	}

	switch w.config.DataType {
	case DataTypeBool:
		offset = alignWithZeroes(buf, offset, desc.Alignment())
		offset = w.bitmap.Finish(int(w.count), offset, buf)
		return offset, desc
	case DataTypeUint32:
		switch {
		case w.maxInt-w.minInt == 0:
			desc = desc.WithEncoding(EncodingConstant)
		case w.maxInt-w.minInt < (1 << 8):
			desc = desc.WithEncoding(EncodingDeltaInt8)
			reduceInts[uint32, uint8](w, align32, binary.LittleEndian.Uint32, 1, putUint8)
		case w.maxInt-w.minInt < (1 << 16):
			desc = desc.WithEncoding(EncodingDeltaInt16)
			reduceInts[uint32, uint16](w, align32, binary.LittleEndian.Uint32, align16, binary.LittleEndian.PutUint16)
		default:
		}
		// The column values.
		offset = alignWithZeroes(buf, offset, desc.Alignment())
		if desc.Encoding() != EncodingDefault {
			binary.LittleEndian.PutUint32(buf[offset:], uint32(w.minInt))
			offset += align32
		}
		if desc.Encoding() != EncodingConstant {
			offset += uint32(copy(buf[offset:], w.data))
		}
		return offset, desc
	case DataTypeUint64:
		switch {
		case w.maxInt-w.minInt == 0:
			desc = desc.WithEncoding(EncodingConstant)
		case w.maxInt-w.minInt < (1 << 8):
			desc = desc.WithEncoding(EncodingDeltaInt8)
			reduceInts[uint64, uint8](w, align64, binary.LittleEndian.Uint64, 1, putUint8)
		case w.maxInt-w.minInt < (1 << 16):
			desc = desc.WithEncoding(EncodingDeltaInt16)
			reduceInts[uint64, uint16](w, align64, binary.LittleEndian.Uint64, align16, binary.LittleEndian.PutUint16)
		case w.maxInt-w.minInt < (1 << 32):
			desc = desc.WithEncoding(EncodingDeltaInt32)
			reduceInts[uint64, uint32](w, align64, binary.LittleEndian.Uint64, align32, binary.LittleEndian.PutUint32)
		default:
		}
		// The column values.
		offset = alignWithZeroes(buf, offset, desc.Alignment())
		if desc.Encoding() != EncodingDefault {
			binary.LittleEndian.PutUint64(buf[offset:], uint64(w.minInt))
			offset += align64
		}
		if desc.Encoding() != EncodingConstant {
			offset += uint32(copy(buf[offset:], w.data))
		}
		return offset, desc
	case DataTypeBytes:
		offset = w.bytes.Finish(offset, buf)
	case DataTypePrefixBytes:
		offset = w.bytes.Finish(offset, buf)
	}

	// The column values.
	offset = alignWithZeroes(buf, offset, desc.Alignment())
	offset += uint32(copy(buf[offset:], w.data))
	return offset, desc
}

func (w *columnBuilder) determineColumnDesc() ColumnDesc {
	desc := ColumnDesc(w.config.DataType)
	if w.nullCount == w.count {
		return desc.WithEncoding(EncodingAllNull)
	}
	if w.nullCount > 0 {
		// Include a NULL-bitmap.
		desc = desc.WithNullBitmap()
	}
	switch w.config.DataType {
	case DataTypeUint32:
		switch {
		case w.maxInt-w.minInt == 0:
			desc = desc.WithEncoding(EncodingConstant)
		case w.maxInt-w.minInt < (1 << 8):
			desc = desc.WithEncoding(EncodingDeltaInt8)
		case w.maxInt-w.minInt < (1 << 16):
			desc = desc.WithEncoding(EncodingDeltaInt16)
		default:
		}
	case DataTypeUint64:
		switch {
		case w.maxInt-w.minInt == 0:
			desc = desc.WithEncoding(EncodingConstant)
		case w.maxInt-w.minInt < (1 << 8):
			desc = desc.WithEncoding(EncodingDeltaInt8)
		case w.maxInt-w.minInt < (1 << 16):
			desc = desc.WithEncoding(EncodingDeltaInt16)
		case w.maxInt-w.minInt < (1 << 32):
			desc = desc.WithEncoding(EncodingDeltaInt32)
		default:
		}
	}
	return desc
}

func (w *columnBuilder) size(offset uint32) uint32 {
	startOffset := offset
	if w.nullCount == w.count {
		return 0 // EncodingAllNull
	}
	if w.nullCount > 0 {
		// The NULL-bitmap.
		offset = w.nulls.Size(offset)
	}

	alignment := dataTypeAlignment[w.config.DataType]
	switch w.config.DataType {
	case DataTypeBool:
		offset = align(offset, alignment)
		offset += uint32(bitmapRequiredSize(int(w.count)))
	case DataTypeUint32:
		offset = align(offset, alignment)
		switch {
		case w.maxInt-w.minInt == 0:
			offset += align32
		case w.maxInt-w.minInt < (1 << 8):
			offset += align32 + w.count - w.nullCount
		case w.maxInt-w.minInt < (1 << 16):
			offset += align32 + (w.count-w.nullCount)<<align16Shift
		default:
			offset += uint32(len(w.data))
		}
	case DataTypeUint64:
		offset = align(offset, alignment)
		switch {
		case w.maxInt-w.minInt == 0:
			offset += align64
		case w.maxInt-w.minInt < (1 << 8):
			offset += align64 + (w.count - w.nullCount)
		case w.maxInt-w.minInt < (1 << 16):
			offset += align64 + (w.count-w.nullCount)<<align16Shift
		case w.maxInt-w.minInt < (1 << 32):
			offset += align64 + (w.count-w.nullCount)<<align32Shift
		default:
			offset += uint32(len(w.data))
		}
	case DataTypePrefixBytes:
		offset = w.bytes.Size(offset)
	case DataTypeBytes:
		offset = w.bytes.Size(offset)
	default:
		// The column values.
		offset = align(offset, alignment)
		offset += uint32(len(w.data))
	}
	return offset - startOffset
}

func putUint8(buf []byte, v uint8) {
	buf[0] = v
}

func reduceInts[O constraints.Integer, N constraints.Integer](
	w *columnBuilder, oldWidth int, readOld func([]byte) O, newWidth int, writeNew func([]byte, N),
) {
	minValue := O(w.minInt)
	dst := 0
	for i := 0; i < len(w.data); i += oldWidth {
		writeNew(w.data[dst:dst+newWidth], N(readOld(w.data[i:i+oldWidth])-minValue))
		dst += newWidth
	}
	w.data = w.data[:dst]
}

// blockWriter TODO(peter) ...
type blockWriter struct {
	vers Version
	cols []columnBuilder
	buf  []byte
	// customHeaderSize may be set by the user to reserve space in the block
	// before the ordinary columnar block header for any fixed-width custom
	// data. At read-time, the user must know the size of the custom data.
	customHeaderSize int
}

// blockWriter implements the ColumnWriter interface.
var _ ColumnWriter = (*blockWriter)(nil)

func (w *blockWriter) init(v Version, customHeaderSize int, configs []ColumnConfig) {
	w.vers = v
	w.customHeaderSize = customHeaderSize
	w.cols = make([]columnBuilder, len(configs))
	for i := range configs {
		w.cols[i].config = configs[i]
	}
	w.reset()
}

func (w *blockWriter) reset() {
	for i := range w.cols {
		w.cols[i].reset()
	}
	w.buf = w.buf[:0]
}

// PutBitmap TODO(peter) ...
func (w *blockWriter) PutBitmap(col int, v bool) {
	w.cols[col].putBitmap(v)
}

// PutUint8 TODO(peter) ...
func (w *blockWriter) PutUint8(col int, v uint8) {
	w.cols[col].putUint8(v)
}

// PutUint16 TODO(peter) ...
func (w *blockWriter) PutUint16(col int, v uint16) {
	w.cols[col].putUint16(v)
}

func (w *blockWriter) PutUint32(col int, v uint32) {
	w.cols[col].putUint32(v)
}

// PutUint64 TODO(peter) ...
func (w *blockWriter) PutUint64(col int, v uint64) {
	w.cols[col].putUint64(v)
}

// PutRawBytes TODO(peter) ...
func (w *blockWriter) PutRawBytes(col int, v []byte) {
	w.cols[col].putRawBytes(v)
}

// PutRawBytesConcat TODO(peter) ...
func (w *blockWriter) PutRawBytesConcat(col int, v1, v2 []byte) {
	w.cols[col].putRawBytesConcat(v1, v2)
}

// PutBytes TODO(peter) ...
func (w *blockWriter) PutPrefixBytes(col int, v []byte) (samePrefix bool) {
	return w.cols[col].putPrefixBytes(v)
}

// PutNull TODO(peter) ...
func (w *blockWriter) PutNull(col int) {
	w.cols[col].putNull()
}

// Finish TODO(peter) ...
func (w *blockWriter) Finish(rows uint32) []byte {
	size := w.Size()
	if cap(w.buf) < size {
		w.buf = make([]byte, size)
	}
	w.buf = w.buf[:size]
	n := len(w.cols)
	binary.LittleEndian.PutUint16(w.buf[w.customHeaderSize:], uint16(n))
	binary.LittleEndian.PutUint32(w.buf[align16+w.customHeaderSize:], uint32(rows))
	pageOffset := blockHeaderSize(n, w.customHeaderSize)
	for i := range w.cols {
		col := &w.cols[i]
		buf := w.buf[blockHeaderSize(i, w.customHeaderSize):]

		binary.LittleEndian.PutUint32(buf[1:], uint32(pageOffset))
		newPageOffset, desc := col.encode(pageOffset, w.buf)
		buf[0] = byte(desc)
		pageOffset = newPageOffset
	}

	// The Go GC which requires that all pointers point within allocated memory
	// chunks. A pointer to the end of an allocated piece of memory technically
	// falls outside of that piece of memory.
	//
	// If the data for the last column is empty and we don't do anything else,
	// the start offset of the last column's data section would fall outside of
	// the allocated memory. We tack on an extra byte encoding the version to
	// resolve this.
	w.buf[pageOffset] = uint8(w.vers)
	pageOffset++
	if len(w.buf) != int(pageOffset) {
		panicf("expected block encoded size %d, but found %d", len(w.buf), pageOffset)
	}
	return w.buf
}

// Size TODO(peter) ...
func (w *blockWriter) Size() int {
	size := blockHeaderSize(len(w.cols), w.customHeaderSize)
	for i := range w.cols {
		size += w.cols[i].size(size)
	}
	// The +1 is the trailing version byte.
	size++
	return int(size)
}

// String outputs a human-readable summary of internal blockWriter state.
func (w *blockWriter) String() string {
	var buf strings.Builder
	size := uint32(w.Size())
	fmt.Fprintf(&buf, "size=%d header=%d:",
		size, blockHeaderSize(len(w.cols), w.customHeaderSize))
	for i := range w.cols {
		fmt.Fprintln(&buf)

		colSize := w.cols[i].size(size)
		fmt.Fprintf(&buf, "%d:%-12s %s,rows=%d,size=%d,nulls=%d",
			i, w.cols[i].config, w.cols[i].determineColumnDesc().Encoding(), w.cols[i].count, colSize, w.cols[i].nullCount)
		switch w.cols[i].config.DataType {
		case DataTypeBytes:
			if len(w.cols[i].bytes.offsets) > 0 {
				if shared := w.cols[i].bytes.offsets[0]; shared > 0 {
					fmt.Fprintf(&buf, ",shared=%d", shared)
				}
			}
			fmt.Fprintf(&buf, ",n16=%d,n32=%d", w.cols[i].bytes.nOffsets16,
				len(w.cols[i].bytes.offsets)-int(w.cols[i].bytes.nOffsets16))
		}
		size += colSize
	}
	return buf.String()
}

// BlockReader TODO(peter) ...
type BlockReader struct {
	start            unsafe.Pointer
	len              uint32
	header           Header
	customHeaderSize uint32
}

// NewBlockReader return a new BlockReader configured to read from the specified
// memory. The caller must ensure that the data is formatted as to the block
// layout specification.
func NewBlockReader(data []byte, customHeaderSize uint32) *BlockReader {
	r := &BlockReader{}
	r.init(data, customHeaderSize)
	return r
}

func (r *BlockReader) init(data []byte, customHeaderSize uint32) {
	*r = BlockReader{
		start:            unsafe.Pointer(&data[0]),
		len:              uint32(len(data)),
		header:           ReadHeader(data[customHeaderSize:]),
		customHeaderSize: customHeaderSize,
	}
}

func (r *BlockReader) columnTag(col int) uint8 {
	if uint16(col) >= r.header.Columns {
		panic("not reached")
	}
	return *(*uint8)(unsafe.Pointer(uintptr(r.start) + uintptr(r.customHeaderSize) + 6 + 5*uintptr(col)))
}

func (r *BlockReader) pageStart(col int) uint32 {
	if uint16(col) >= r.header.Columns {
		return r.len
	}
	return binary.LittleEndian.Uint32(
		unsafe.Slice((*byte)(unsafe.Pointer(r.pointer(r.customHeaderSize+uint32(6+5*col+1)))), 4))
}

func (r *BlockReader) pointer(offset uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(r.start) + uintptr(offset))
}

func (r *BlockReader) data() []byte {
	return unsafe.Slice((*byte)(r.start), r.len)
}

func (r *BlockReader) column(col, n int) Vec {
	tag := r.columnTag(col)
	start := r.pageStart(col)
	end := r.pageStart(col + 1)

	var v Vec
	v.N = uint32(n)
	v.Desc = ColumnDesc(tag)
	v.DataType = v.Desc.DataType()
	v.Encoding = v.Desc.Encoding()

	// The NULL-bitmap.
	switch {
	case v.Encoding == EncodingAllNull:
		v.NullBitmap = makeNullBitmap(allNullsSentinel)
	case !v.Desc.HasNullBitmap():
		v.NullBitmap = makeNullBitmap(nil)
	default:
		start = align(start, align32)
		v.NullBitmap = makeNullBitmapRaw(r.pointer(start))
		// Skip over the null bitmap. The /16 and *4 cancel out to a shift by 2 (/4).
		// start += 4 * (uint32(r.header.Rows+15) / 16)
		start += uint32(r.header.Rows+15) >> 2
	}

	start = align(start, v.Desc.Alignment())

	switch v.Encoding {
	case EncodingDefault:
	case EncodingAllNull:
	case EncodingConstant:
		v.Min = v.DataType.readInteger(r.pointer(start))
		start += uint32(v.DataType.fixedWidth())
	case EncodingDeltaInt8:
		v.Min = v.DataType.readInteger(r.pointer(start))
		start += uint32(v.DataType.fixedWidth())
	case EncodingDeltaInt16:
		v.Min = v.DataType.readInteger(r.pointer(start))
		start += uint32(v.DataType.fixedWidth())
	case EncodingDeltaInt32:
		v.Min = v.DataType.readInteger(r.pointer(start))
		start += uint32(v.DataType.fixedWidth())
	default:
		panic("unreachable")
	}

	// The column values. The pointer to this data cannot point to the end of
	// the data block as doing so violates checks in the Go GC. So if the data
	// section is empty we set the data pointer to an empty byte slice.
	if end-start == 0 {
		v.start = unsafe.Pointer(unsafe.SliceData(emptyBytesData))
	} else {
		v.start = r.pointer(start)
	}
	return v
}

// Column returns a Vector for the specified column. The caller must check (or
// otherwise know) the type of the column before accessing the column data. The
// caller should check to see if the column contains any NULL values
// (Vec.Null.Empty()) and specialize processing accordingly.
func (r *BlockReader) Column(col int) Vec {
	if col < 0 || uint16(col) >= r.header.Columns {
		panic("invalid column")
	}
	return r.column(col, int(r.header.Rows))
}

func (r *BlockReader) headerToBinFormatter(f *binfmt.Formatter) {
	f.CommentLine("columnar block header")
	f.HexBytesln(2, "%d columns", r.header.Columns)
	f.HexBytesln(4, "%d rows", r.header.Rows)

	for i := 0; i < int(r.header.Columns); i++ {
		col := r.Column(i)
		f.CommentLine("column %d", i)
		f.Byte("%s", col.Desc)
		f.HexBytesln(4, "page start %d", r.pageStart(i))
	}
}

func (r *BlockReader) columnToBinFormatter(f *binfmt.Formatter, col, rows int) {
	f.CommentLine("Data for column %d", col)
	desc := ColumnDesc(r.columnTag(col))
	if desc.Encoding() == EncodingAllNull {
		return
	}

	start := r.pageStart(col)
	end := r.pageStart(col + 1)
	endOff := f.Offset() + int(end-start)
	vec := r.column(col, rows)

	if desc.HasNullBitmap() {
		if padding := alignPadding(uint32(uintptr(f.Pointer(0))), align32); padding > 0 {
			f.HexBytesln(int(padding), "padding to align to 32-bit")
		}
		nullBitmapToBinFormatter(f, rows)
	}
	if padding := alignPadding(uint32(uintptr(f.Pointer(0))), desc.Alignment()); padding > 0 {
		f.HexBytesln(int(padding), "padding to align to %d-bit", desc.Alignment()*8)
	}

	switch desc.DataType() {
	case DataTypeBool:
		bitmapToBinFormatter(f, rows)
	case DataTypeUint8, DataTypeUint16:
		if w := desc.RowWidth(); w > 0 {
			for i := 0; i < rows; i++ {
				f.HexBytesln(w, "data[%d] = %d", i, f.PeekInt(w))
			}
		}
	case DataTypeUint32:
		if desc.Encoding() != EncodingDefault {
			f.HexBytesln(4, "32-bit constant: %d", uint32(vec.Min))
		}
		if w := desc.RowWidth(); w > 0 {
			for i := 0; i < rows; i++ {
				f.HexBytesln(w, "data[%d] = %d", i, f.PeekInt(w))
			}
		}
	case DataTypeUint64:
		if desc.Encoding() != EncodingDefault {
			f.HexBytesln(8, "64-bit constant: %d", uint32(vec.Min))
		}
		if w := desc.RowWidth(); w > 0 {
			for i := 0; i < rows; i++ {
				f.HexBytesln(w, "data[%d] = %d", i, f.PeekInt(w))
			}
		}
	case DataTypeBytes:
		rawBytesToBinFormatter(f, uint32(rows), nil)

		// case DataTypePrefixBytes:
		// TODO(jackson): DataTypePrefixBytes
	}

	// If there are any bytes in the column data section that haven't been
	// formatted yet, just hex dump them.
	if v := endOff - f.Offset(); v > 0 {
		f.HexBytesln(v, "???")
	}
}
