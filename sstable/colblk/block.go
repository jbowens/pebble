// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
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

// WriteHeader TODO(jackson) ...
func WriteHeader(dest []byte, h Header) {
	binary.LittleEndian.PutUint16(dest, h.Columns)
	binary.LittleEndian.PutUint32(dest[align16:], h.Rows)
}

// FinishBlock writes the columnar block to a byte slice. FinishBlock assumes
// all columns have the same number of rows. If that's not the case, the caller
// should manually construct their own block.
func FinishBlock(rows int, writers []ColumnWriter) []byte {
	size := blockHeaderSize(len(writers), 0)
	for _, cw := range writers {
		size = cw.Size(rows, size)
	}
	size++ // +1 for the trailing version byte.

	buf := make([]byte, size)
	WriteHeader(buf, Header{Columns: uint16(len(writers)), Rows: uint32(rows)})
	pageOffset := blockHeaderSize(len(writers), 0)
	for col, cw := range writers {
		hi := blockHeaderSize(col, 0)
		binary.LittleEndian.PutUint32(buf[hi+1:], pageOffset)
		var desc ColumnDesc
		pageOffset, desc = cw.Finish(rows, pageOffset, buf)
		buf[hi] = byte(desc)
	}
	return buf
}

var emptyBytesData = make([]byte, 0)

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
	case EncodingConstant, EncodingDeltaInt8, EncodingDeltaInt16, EncodingDeltaInt32:
		switch v.DataType {
		case DataTypeUint8:
			v.Min = uint64(*(*uint8)(r.pointer(start)))
		case DataTypeUint16:
			v.Min = uint64(*(*uint16)(r.pointer(start)))
		case DataTypeUint32:
			v.Min = uint64(*(*uint32)(r.pointer(start)))
		case DataTypeUint64:
			v.Min = *(*uint64)(r.pointer(start))
		default:
			panic(fmt.Sprintf("non-integer type %s", v.DataType))
		}
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

	nonNullRows := rows
	if desc.HasNullBitmap() {
		if padding := alignPadding(uint32(uintptr(f.Pointer(0))), align32); padding > 0 {
			f.HexBytesln(int(padding), "padding to align to 32-bit")
		}
		nullBitmapToBinFormatter(f, rows)
		nonNullRows = vec.NullBitmap.count(rows)
	}
	if padding := alignPadding(uint32(uintptr(f.Pointer(0))), desc.Alignment()); padding > 0 {
		f.HexBytesln(int(padding), "padding to align to %d-bit", desc.Alignment()*8)
	}

	switch dt := desc.DataType(); dt {
	case DataTypeBool:
		bitmapToBinFormatter(f, rows)
	case DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		logicalWidth := dt.fixedWidth()
		if desc.Encoding() != EncodingDefault {
			f.HexBytesln(logicalWidth, "%d-bit constant: %d", logicalWidth*8, vec.Min)
		}
		if w := desc.RowWidth(); w > 0 {
			for i := 0; i < nonNullRows; i++ {
				f.HexBytesln(w, "data[%d] = %d", i, f.PeekInt(w))
			}
		}
	case DataTypeBytes:
		rawBytesToBinFormatter(f, uint32(nonNullRows), nil)

		// case DataTypePrefixBytes:
		// TODO(jackson): DataTypePrefixBytes
	}

	switch v := endOff - f.Offset(); cmp.Compare[int](v, 0) {
	case +1:
		// There are bytes in the column data section that haven't been
		// formatted yet. Just hex dump them.
		f.HexBytesln(v, "???")
	case 0:
	case -1:
		panicf("expected f.Offset() = %d, but found %d; did column %s format too many bytes?", endOff, f.Offset(), desc)
	}

}
