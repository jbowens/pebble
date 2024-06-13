// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
	"golang.org/x/exp/constraints"
)

// DataType describes the logical type of a column's values. Some data types
// have multiple possible physical representations. Encoding a column may choose
// between possible physical representations depending on the distribution of
// values and the size of the resulting physical representation.
type DataType uint8

const (
	DataTypeInvalid     DataType = 0
	DataTypeBool        DataType = 1
	DataTypeUint8       DataType = 2
	DataTypeUint16      DataType = 3
	DataTypeUint32      DataType = 4
	DataTypeUint64      DataType = 5
	DataTypeBytes       DataType = 6
	DataTypePrefixBytes DataType = 7
	dataTypesCount      DataType = 8
)

var dataTypeName [dataTypesCount]string = [dataTypesCount]string{
	DataTypeInvalid:     "invalid",
	DataTypeBool:        "bool",
	DataTypeUint8:       "int8",
	DataTypeUint16:      "int16",
	DataTypeUint32:      "int32",
	DataTypeUint64:      "int64",
	DataTypeBytes:       "bytes",
	DataTypePrefixBytes: "prefixbytes",
}

var dataTypeAlignment [dataTypesCount]uint32 = [dataTypesCount]uint32{
	DataTypeInvalid:     0,
	DataTypeBool:        8,
	DataTypeUint8:       1,
	DataTypeUint16:      2,
	DataTypeUint32:      4,
	DataTypeUint64:      8,
	DataTypeBytes:       1,
	DataTypePrefixBytes: 1,
}

// String returns a human-readable string representation of the data type.
func (t DataType) String() string {
	return dataTypeName[t]
}

func (t DataType) fixedWidth() int {
	switch t {
	case DataTypeBool:
		return 1
	case DataTypeUint8:
		return 1
	case DataTypeUint16:
		return 2
	case DataTypeUint32:
		return 4
	case DataTypeUint64:
		return 8
	default:
		panic(fmt.Sprintf("non-fixed width type %s", t))
	}
}

// DataTypes represents a slice of DataType values.
type DataTypes []DataType

// String returns the concatenated string representations of data types.
func (dt DataTypes) String() string {
	var buf bytes.Buffer
	for i := range dt {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(dt[i].String())
	}
	return buf.String()
}

const (
	columnDescDataTypeMask      = (1 << 3) - 1
	columnDescNullBitmapFlagBit = (1 << 4)
	columnDescEncodingShift     = 5
	columnDescEncodingMask      = 0b00011111
)

// ColumnDesc describes the column's data type and its encoding.
//
//	high                                       low
//	  X     X     X     X     X     X     X     X
//	 \_____________/    |     |    \_____________/
//	    encoding        |   unused    data type
//	                    |
//	               null bitmap?
//
// Data type (bits 0, 1, 2):
//
// The data type is stored in the low 3 bits. It indicates the logical data type
// of the values stored within the column.
//
// Unused (bit 3):
//
// The fourth low bit is unused and reserved for future use. It may be used to
// expand the set of data types in future extensions.
//
// Null bitmap (bit 4):
//
// The bit at position 4 indicates whether the column is prefixed with a null
// bitmap. A null bitmap is a bitmap where each bit corresponds to a row in the
// column, and a set bit indicates that the corresponding row is NULL.
//
// Column encoding (bits 5, 6, 7):
//
// The bits at positions 5, 6, and 7 encode an enum describing the encoding of
// the column. See the ColumnEncoding type and its constants for details.
type ColumnDesc uint8

// DataType returns the logical data type encoded.
func (d ColumnDesc) DataType() DataType {
	// The data type is stored within the first 3 bits.
	return DataType(d & columnDescDataTypeMask)
}

// HasNullBitmap returns true if the column encodes a NULL bitmap.
func (d ColumnDesc) HasNullBitmap() bool {
	return d&columnDescNullBitmapFlagBit != 0
}

// Encoding returns the column's encoding.
func (d ColumnDesc) Encoding() ColumnEncoding {
	return ColumnEncoding(d >> columnDescEncodingShift)
}

// RowWidth returns the per-row width in bytes, if the column has a fixed
// per-row width. If the column is delta-encoded, this is the width of the
// per-row deltas.
func (d ColumnDesc) RowWidth() int {
	switch d.Encoding() {
	case EncodingConstant:
		return 0
	case EncodingDeltaInt8:
		return 1
	case EncodingDeltaInt16:
		return 2
	case EncodingDeltaInt32:
		return 4
	default:
		return d.DataType().fixedWidth()
	}

}

// Alignment returns the required alignment for the column.
func (e ColumnDesc) Alignment() uint32 {
	return dataTypeAlignment[e.DataType()]
}

// WithNullBitmap returns the ColumnDesc with the null bitmap flag set.
func (d ColumnDesc) WithNullBitmap() ColumnDesc {
	return d | columnDescNullBitmapFlagBit
}

// WithEncoding returns the column description with the provided encoding.
func (d ColumnDesc) WithEncoding(enc ColumnEncoding) ColumnDesc {
	return d | ColumnDesc(enc&columnDescEncodingMask)<<columnDescEncodingShift
}

// String returns a human-readable string describing the column encoding.
func (d ColumnDesc) String() string {
	var sb strings.Builder
	dt := d.DataType()
	fmt.Fprint(&sb, dt.String())
	if d.HasNullBitmap() {
		fmt.Fprint(&sb, "+nullbitmap")
	}
	if enc := d.Encoding(); enc != EncodingDefault {
		fmt.Fprintf(&sb, "+%s", enc)
	}
	return sb.String()
}

// ColumnDescs is a slice of ColumnDesc values.
type ColumnDescs []ColumnDesc

// String returns the concatenated string representations of the column
// descriptions.
func (c ColumnDescs) String() string {
	var buf bytes.Buffer
	for i := range c {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(c[i].String())
	}
	return buf.String()
}

// ColumnEncoding is an enum describing the encoding of a column.
type ColumnEncoding uint8

const (
	// EncodingDefault indicates that the default encoding is in-use for a
	// column, encoding n values for n rows.
	EncodingDefault ColumnEncoding = 0
	// EncodingAllNull indicates that the column is entirely composed of
	// NULL values and no data is encoded..
	EncodingAllNull ColumnEncoding = 1
	// EncodingConstant indicates that all of the non-NULL values of the column
	// are a single constant. After the NULL bitmap (if present), the column
	// data encodes a single value of the same type as the column's data type.
	// Eg, a DataTypeInt32 column with EncodingConstant encodes a single 32-bit
	// integer. Columns with the DataTypeBytes encoding encode a 32-bit integer
	// length, followed by the constant's bytes.
	EncodingConstant ColumnEncoding = 2
	// EncodingDeltaInt8 indicates that the integer column is encoded using a
	// delta encoding whereby individual rows' non-NULL values are stored as
	// deltas relative to a constant. Column data is prefixed with a fixed-width
	// integer constant value of the column's data type. The constant is
	// followed by N fixed-width int8 values. A non-NULL value is computed by
	// summing the constant with the corresponding row's int8 value.
	EncodingDeltaInt8 ColumnEncoding = 3
	// EncodingDeltaInt16 indicates that the integer column is encoded using a
	// delta encoding whereby individual rows' non-NULL values are stored as
	// deltas relative to a constant. Column data is prefixed with a fixed-width
	// integer constant value of the column's data type. The constant is
	// followed by N fixed-width int16 values. A non-NULL value is computed by
	// summing the constant with the corresponding row's int16 value.
	EncodingDeltaInt16 ColumnEncoding = 4
	// EncodingDeltaInt32 indicates that the integer column is encoded using a
	// delta encoding whereby individual rows' non-NULL values are stored as
	// deltas relative to a constant. Column data is prefixed with a fixed-width
	// integer constant value of the column's data type. The constant is
	// followed by N fixed-width int32 values. A non-NULL value is computed by
	// summing the constant with the corresponding row's int32 value.
	EncodingDeltaInt32 ColumnEncoding = 5

	encodingTypeCount = 7
)

// String returns the string representation of the column encoding.
func (e ColumnEncoding) String() string {
	return encodingName[e]
}

var encodingName [encodingTypeCount]string = [encodingTypeCount]string{
	EncodingDefault:    "default",
	EncodingAllNull:    "allnull",
	EncodingConstant:   "constant",
	EncodingDeltaInt8:  "delta8",
	EncodingDeltaInt16: "delta16",
	EncodingDeltaInt32: "delta32",
}

// ColumnWriter is an interface implemented by column encoders that accumulate a
// column's values and then serialize them.
type ColumnWriter interface {
	Encoder
	// NumColumns returns the number of columns the ColumnWriter will encode.
	NumColumns() int
	// Finish serializes the column at the specified index, writing the column's
	// data to buf at offset, and returning the offset at which the next column
	// should be encoded. Finish also returns a column descriptor describing the
	// encoding of the column, which will be serialized within the block header.
	// The column index must be less than NumColumns().
	//
	// Finish is called for each index < NumColumns() in order.
	Finish(col int, rows int, offset uint32, buf []byte) (uint32, ColumnDesc)
}

// Encoder is an interface implemented by column encoders.
type Encoder interface {
	// Reset clears the ColumnWriter's internal state, preparing it for reuse.
	Reset()
	// Size returns the size required to encode the column's current values.
	//
	// The `rows` argument must be the current number of logical rows in the
	// column.  Some implementations support defaults, and these implementations
	// rely on the caller to inform them the current number of logical rows. The
	// provided `rows` must be greater than or equal to the largest row set + 1.
	// In other words, Size does not support determining the size of a column's
	// earlier sizze before additional rows were added.
	Size(rows int, offset uint32) uint32
	// WriteDebug writes a human-readable description of the current column
	// state to the provided writer.
	WriteDebug(w io.Writer, rows int)
}

func MakeNullable[W ColumnWriter](dataType DataType, w W) Nullable[W] {
	return Nullable[W]{
		dataType: dataType,
		writer:   w,
	}
}

type Nullable[W ColumnWriter] struct {
	dataType  DataType
	writer    W
	nulls     nullBitmapBuilder
	nullCount int
}

func (n *Nullable[W]) SetNull(row int) {
	n.nulls = n.nulls.Set(row, true)
	n.nullCount++
}

func (n *Nullable[W]) NotNull(row int) (W, int) {
	return n.writer, row - n.nullCount
}

func (n *Nullable[W]) NumColumns() int { return n.writer.NumColumns() }

func (n *Nullable[W]) Reset() {
	for i := range n.nulls {
		n.nulls[i] = 0
	}
	n.nulls = n.nulls[:0]
	n.nullCount = 0
	n.writer.Reset()
}

func (n *Nullable[W]) Size(rows int, offset uint32) uint32 {
	if n.nullCount == rows {
		// All NULLs; doesn't need to be encoded into column data.
		return offset
	} else if n.nullCount > 0 {
		offset = n.nulls.Size(offset)
	}
	return n.writer.Size(rows-n.nullCount, offset)
}

func (n *Nullable[W]) Finish(col int, rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	if n.nullCount == rows {
		return offset, ColumnDesc(n.dataType).WithEncoding(EncodingAllNull)
	} else if n.nullCount > 0 {
		offset = n.nulls.Finish(offset, buf)
		off, desc := n.writer.Finish(col, rows-n.nullCount, offset, buf)
		return off, desc.WithNullBitmap()
	}
	return n.writer.Finish(col, rows-n.nullCount, offset, buf)
}

func (n *Nullable[W]) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "nullable[%d nulls](", n.nullCount)
	n.writer.WriteDebug(w, rows-n.nullCount)
	fmt.Fprintf(w, ")")
}

func MakeDefaultNull[W ColumnWriter](dataType DataType, w W) DefaultNull[W] {
	return DefaultNull[W]{
		dataType: dataType,
		writer:   w,
	}
}

type DefaultNull[W ColumnWriter] struct {
	dataType     DataType
	writer       W
	nulls        nullBitmapBuilder
	idx          int
	notNullCount int
}

func (n *DefaultNull[W]) NotNull(row int) (W, int) {
	for r := n.idx; r < row; r++ {
		n.nulls = n.nulls.Set(r, true)
	}
	n.nulls = n.nulls.Set(row, false)
	n.idx = row + 1
	n.notNullCount++
	return n.writer, n.notNullCount - 1
}

func (n *DefaultNull[W]) NumColumns() int { return n.writer.NumColumns() }

func (n *DefaultNull[W]) Reset() {
	n.nulls = n.nulls[:0]
	n.idx = 0
	n.notNullCount = 0
	n.writer.Reset()
}

func (n *DefaultNull[W]) Size(rows int, offset uint32) uint32 {
	if n.notNullCount == 0 {
		// All NULLs; doesn't need to be encoded into column data.
		return offset
	} else if n.notNullCount != rows {
		// Need a null bitmap.
		offset = n.nulls.Size(offset)
	}
	return n.writer.Size(n.notNullCount, offset)
}

func (n *DefaultNull[W]) Finish(col, rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	if n.notNullCount == 0 {
		return offset, ColumnDesc(n.dataType).WithEncoding(EncodingAllNull)
	} else if n.notNullCount != rows {
		offset = n.nulls.Finish(offset, buf)
		offset, desc := n.writer.Finish(col, n.notNullCount, offset, buf)
		return offset, desc.WithNullBitmap()
	}
	return n.writer.Finish(col, n.notNullCount, offset, buf)
}

func (n *DefaultNull[W]) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "defaultnull[%d not null](", n.notNullCount)
	n.writer.WriteDebug(w, n.notNullCount)
	fmt.Fprintf(w, ")")
}

// UintDefault configures the behavior of a [Uint64Builder], [Uint32Builder],
// [Uint16Builder] or [Uint8Builder] with respect to unspecified values.
type UintDefault int8

const (
	// UintDefaultNone indicates that there is no default value, and that the
	// caller must call Set for every row.
	UintDefaultNone UintDefault = iota
	// UintDefaultZero indicates that there is a default value of 0. The caller
	// may choose to omit calls to Set for rows with value zero. During delta
	// encoding, all deltas will be computed relative to zero. This may result
	// in a less efficient encoding if every value in the column is non-zero.
	// UintDefaultZero may improve performance of encoding columns with many
	// zero values by allowing the caller to omit Set calls.
	UintDefaultZero
)

type Uint64Builder struct {
	defaultConfig UintDefault
	n             int
	minimum       uint64
	maximum       uint64
	width         int // 0, 1, 2, 4, or 8
	minMaxRow     int // index of last update to minimum or maximum
	elems         UnsafeRawSlice[uint64]
}

func (b *Uint64Builder) Init(defaultConfig UintDefault) {
	b.defaultConfig = defaultConfig
	b.Reset()
}

func (b *Uint64Builder) NumColumns() int { return 1 }

func (b *Uint64Builder) Reset() {
	if b.defaultConfig == UintDefaultZero {
		b.minimum = 0
		b.maximum = 0
		for i := 0; i < b.n; i++ {
			b.elems.set(i, 0)
		}
	} else {
		b.minimum = math.MaxUint64
		b.maximum = 0
	}
	b.minMaxRow = 0
	b.width = 0
}

func (b *Uint64Builder) Set(row int, v uint64) {
	if b.n <= row {
		// Double the size of the buffer, or initialize it to at least 256 bytes
		// if this is the first allocation. Then double until there's sufficient
		// space for n bytes.
		n2 := max(b.n<<1, int(256>>align64Shift))
		for n2 <= row {
			n2 <<= 1 /* double the size */
		}
		newData := make([]byte, n2<<align64Shift)
		newElems := makeUnsafeRawSlice[uint64](unsafe.Pointer(&newData[0]))
		copy(newElems.Slice(b.n), b.elems.Slice(b.n))
		b.elems = newElems
		b.n = n2
	}
	if b.minimum > v || b.maximum < v {
		b.minimum = min(v, b.minimum)
		b.maximum = max(v, b.maximum)
		b.minMaxRow = row
		b.width = deltaWidth(b.maximum - b.minimum)
	}
	b.elems.set(row, v)
}

func (b *Uint64Builder) Size(rows int, offset uint32) uint32 {
	w := b.width
	if b.minMaxRow > rows-1 {
		minimum, maximum := computeMinMax(b.elems.Slice(rows))
		w = deltaWidth(maximum - minimum)
	}
	offset = align(offset, align64)
	switch w {
	case 0:
		return offset + align64
	case 1:
		return offset + align64 + uint32(rows)
	case align16:
		return offset + align64 + uint32(rows<<align16Shift)
	case align32:
		return offset + align64 + uint32(rows<<align32Shift)
	default:
		return offset + uint32(rows<<align64Shift)
	}
}

func (b *Uint64Builder) Finish(col, rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeUint64)
	offset = alignWithZeroes(buf, offset, align64)

	writeMinimum := func() {
		binary.LittleEndian.PutUint64(buf[offset:], b.minimum)
		offset += align64
	}
	// We can use the incrementally computed width as long as the minimum and
	// maximum haven't been updated by a call to Set a later row.
	w := b.width
	if b.minMaxRow > rows-1 {
		minimum, maximum := computeMinMax(b.elems.Slice(rows))
		w = deltaWidth(maximum - minimum)
	}

	switch w {
	case 0:
		desc = desc.WithEncoding(EncodingConstant)
		writeMinimum()
	case 1:
		desc = desc.WithEncoding(EncodingDeltaInt8)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset]))
		reduceUints[uint64, uint8](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows)
	case align16:
		desc = desc.WithEncoding(EncodingDeltaInt16)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset]))
		reduceUints[uint64, uint16](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows << align16Shift)
	case align32:
		desc = desc.WithEncoding(EncodingDeltaInt32)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint32](unsafe.Pointer(&buf[offset]))
		reduceUints[uint64, uint32](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows << align32Shift)
	default:
		offset += uint32(copy(buf[offset:], unsafe.Slice((*byte)(b.elems.ptr), rows<<align64Shift)))
	}
	return offset, desc
}

func (b *Uint64Builder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "uint64: %d rows", rows)
}

type Uint32Builder struct {
	defaultConfig UintDefault
	n             int
	minimum       uint32
	maximum       uint32
	width         int // 0, 1, 2, 4
	minMaxRow     int // index of last update to minimum or maximum
	elems         UnsafeRawSlice[uint32]
}

func (b *Uint32Builder) Init(defaultConfig UintDefault) {
	b.defaultConfig = defaultConfig
	b.Reset()
}

func (b *Uint32Builder) NumColumns() int { return 1 }

func (b *Uint32Builder) Reset() {
	if b.defaultConfig == UintDefaultZero {
		b.minimum = 0
		b.maximum = 0
		for i := 0; i < b.n; i++ {
			b.elems.set(i, 0)
		}
	} else {
		b.minimum = math.MaxUint32
		b.maximum = 0
	}
	b.minMaxRow = 0
	b.width = 0
}

func (b *Uint32Builder) Set(row int, v uint32) {
	if b.n <= row {
		// Double the size of the buffer, or initialize it to at least 256 bytes
		// if this is the first allocation. Then double until there's sufficient
		// space for n bytes.
		n2 := max(b.n<<1, int(256>>align32Shift))
		for n2 <= row {
			n2 <<= 1 /* double the size */
		}
		newData := make([]byte, n2<<align32Shift)
		newElems := makeUnsafeRawSlice[uint32](unsafe.Pointer(&newData[0]))
		copy(newElems.Slice(b.n), b.elems.Slice(b.n))
		b.elems = newElems
		b.n = n2
	}
	if b.minimum > v || b.maximum < v {
		b.minimum = min(v, b.minimum)
		b.maximum = max(v, b.maximum)
		b.minMaxRow = row
		b.width = deltaWidth(uint64(b.maximum - b.minimum))
	}
	b.elems.set(row, v)
}

func (b *Uint32Builder) Size(rows int, offset uint32) uint32 {
	offset = align(offset, align32)
	w := b.width
	if b.minMaxRow > rows-1 {
		minimum, maximum := computeMinMax(b.elems.Slice(rows))
		w = deltaWidth(uint64(maximum - minimum))
	}
	switch w {
	case 0:
		return offset + align32
	case 1:
		return offset + align32 + uint32(rows)
	case align16:
		return offset + align32 + uint32(rows<<align16Shift)
	case align32:
		return offset + uint32(rows<<align32Shift)
	default:
		panic("unreachable")
	}
}

func (b *Uint32Builder) Finish(col int, rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeUint32)
	offset = alignWithZeroes(buf, offset, align32)
	writeMinimum := func() {
		binary.LittleEndian.PutUint32(buf[offset:], b.minimum)
		offset += align32
	}
	w := b.width
	if b.minMaxRow > rows-1 {
		minimum, maximum := computeMinMax(b.elems.Slice(rows))
		w = deltaWidth(uint64(maximum - minimum))
	}
	switch w {
	case 0:
		desc = desc.WithEncoding(EncodingConstant)
		writeMinimum()
	case 1:
		desc = desc.WithEncoding(EncodingDeltaInt8)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset]))
		reduceUints[uint32, uint8](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows)
	case align16:
		desc = desc.WithEncoding(EncodingDeltaInt16)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset]))
		reduceUints[uint32, uint16](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows << align16Shift)
	case align32:
		offset += uint32(copy(buf[offset:], unsafe.Slice((*byte)(b.elems.ptr), rows<<align32Shift)))
	default:
		panic("unreachable")
	}
	return offset, desc
}

func (b *Uint32Builder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "uint32: %d rows", rows)
}

type Uint16Builder struct {
	defaultConfig UintDefault
	n             int
	minimum       uint16
	maximum       uint16
	width         int // 0, 1, 2
	minMaxRow     int // index of last update to minimum or maximum
	elems         UnsafeRawSlice[uint16]
}

func (b *Uint16Builder) Init(defaultConfig UintDefault) {
	b.defaultConfig = defaultConfig
	b.Reset()
}

func (b *Uint16Builder) NumColumns() int { return 1 }

func (b *Uint16Builder) Reset() {
	if b.defaultConfig == UintDefaultZero {
		b.minimum = 0
		b.maximum = 0
		for i := 0; i < b.n; i++ {
			b.elems.set(i, 0)
		}
	} else {
		b.minimum = math.MaxUint16
		b.maximum = 0
	}
	b.width = 0
	b.minMaxRow = 0
}

func (b *Uint16Builder) Set(row int, v uint16) {
	if b.n <= row {
		// Double the size of the buffer, or initialize it to at least 256 bytes
		// if this is the first allocation. Then double until there's sufficient
		// space for n bytes.
		n2 := max(b.n<<1, int(256>>align16Shift))
		for n2 <= row {
			n2 <<= 1 /* double the size */
		}
		newData := make([]byte, n2<<align16Shift)
		newElems := makeUnsafeRawSlice[uint16](unsafe.Pointer(&newData[0]))
		copy(newElems.Slice(b.n), b.elems.Slice(b.n))
		b.elems = newElems
		b.n = n2
	}
	if b.minimum > v || b.maximum < v {
		b.minimum = min(v, b.minimum)
		b.maximum = max(v, b.maximum)
		b.minMaxRow = row
		b.width = deltaWidth(uint64(b.maximum - b.minimum))
	}
	b.elems.set(row, v)
}

func (b *Uint16Builder) Size(rows int, offset uint32) uint32 {
	offset = align(offset, align16)
	w := b.width
	if b.minMaxRow > rows-1 {
		minimum, maximum := computeMinMax(b.elems.Slice(rows))
		w = deltaWidth(uint64(maximum - minimum))
	}
	switch w {
	case 0:
		return offset + align16
	case 1:
		return offset + align16 + uint32(rows)
	case align16:
		return offset + uint32(rows<<align16Shift)
	default:
		panic("unreachable")
	}
}

func (b *Uint16Builder) Finish(col, rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeUint16)
	offset = alignWithZeroes(buf, offset, align16)
	writeMinimum := func() {
		binary.LittleEndian.PutUint16(buf[offset:], b.minimum)
		offset += align16
	}
	w := b.width
	if b.minMaxRow > rows-1 {
		minimum, maximum := computeMinMax(b.elems.Slice(rows))
		w = deltaWidth(uint64(maximum - minimum))
	}
	switch w {
	case 0:
		desc = desc.WithEncoding(EncodingConstant)
		writeMinimum()
	case 1:
		desc = desc.WithEncoding(EncodingDeltaInt8)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset]))
		reduceUints[uint16, uint8](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows)
	case align16:
		offset += uint32(copy(buf[offset:], unsafe.Slice((*byte)(b.elems.ptr), rows<<align16Shift)))
	default:
		panic("unreachable")
	}
	return offset, desc
}

func (b *Uint16Builder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "uint16: %d rows", rows)
}

type Uint8Builder struct {
	defaultConfig UintDefault
	n             int
	minimum       uint8
	maximum       uint8
	constant      bool
	minMaxRow     int // index of last update to minimum or maximum
	elems         UnsafeRawSlice[uint8]
}

func (b *Uint8Builder) Init(defaultConfig UintDefault) {
	b.defaultConfig = defaultConfig
	b.Reset()
}

func (b *Uint8Builder) NumColumns() int { return 1 }

func (b *Uint8Builder) Reset() {
	if b.defaultConfig == UintDefaultZero {
		b.minimum = 0
		b.maximum = 0
		for i := 0; i < b.n; i++ {
			b.elems.set(i, 0)
		}
	} else {
		b.minimum = math.MaxUint8
		b.maximum = 0
	}
	b.minMaxRow = 0
	b.constant = true
}

func (b *Uint8Builder) Set(row int, v uint8) {
	if b.n <= row {
		// Double the size of the buffer, or initialize it to at least 256 bytes
		// if this is the first allocation. Then double until there's sufficient
		// space for n bytes.
		n2 := max(b.n<<1, 256)
		for n2 <= row {
			n2 <<= 1 /* double the size */
		}
		newData := make([]byte, n2)
		newElems := makeUnsafeRawSlice[uint8](unsafe.Pointer(&newData[0]))
		copy(newElems.Slice(b.n), b.elems.Slice(b.n))
		b.elems = newElems
		b.n = n2
	}
	if b.minimum > v || b.maximum < v {
		b.minimum = min(v, b.minimum)
		b.maximum = max(v, b.maximum)
		b.minMaxRow = row
		b.constant = b.minimum == b.maximum
	}
	b.elems.set(row, v)
}

func (b *Uint8Builder) Size(rows int, offset uint32) uint32 {
	constant := b.constant
	if b.minMaxRow > rows-1 {
		minimum, maximum := computeMinMax(b.elems.Slice(rows))
		constant = minimum == maximum
	}
	if constant {
		return offset + 1
	}
	return offset + uint32(rows)
}

func (b *Uint8Builder) Finish(col, rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeUint8)
	constant := b.constant
	if b.minMaxRow > rows-1 {
		minimum, maximum := computeMinMax(b.elems.Slice(rows))
		constant = minimum == maximum
	}
	if constant {
		buf[offset] = b.minimum
		desc = desc.WithEncoding(EncodingConstant)
		offset++
	}
	offset += uint32(copy(buf[offset:], unsafe.Slice((*byte)(b.elems.ptr), rows)))
	return offset, desc
}

func (b *Uint8Builder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "uint8: %d rows", rows)
}

// reduceUints reduces the bit-width of a slice of unsigned by subtracting a
// minimum value from each element and writing it to dst. For example,
//
//	reduceUints[uint64, uint8](10, []uint64{10, 11, 12}, dst)
//
// could be used to reduce a slice of uint64 values to uint8 values {0, 1, 2}.
func reduceUints[O constraints.Integer, N constraints.Integer](minimum O, values []O, dst []N) {
	for i := 0; i < len(values); i++ {
		dst[i] = N(values[i] - minimum)
	}
}

func computeMinMax[I constraints.Unsigned](values []I) (I, I) {
	minimum := I(0) - 1
	maximum := I(0)
	for _, v := range values {
		if v < minimum {
			minimum = v
		}
		if v > maximum {
			maximum = v
		}
	}
	return minimum, maximum
}

func deltaWidth(delta uint64) int {
	switch {
	case delta == 0:
		return 0
	case delta < (1 << 8):
		return 1
	case delta < (1 << 16):
		return align16
	case delta < (1 << 32):
		return align32
	default:
		return align64
	}
}

func uintsToBinFormatter(f *binfmt.Formatter, rows int, vec Vec) {
	logicalWidth := vec.Desc.DataType().fixedWidth()
	if vec.Desc.Encoding() != EncodingDefault {
		f.HexBytesln(logicalWidth, "%d-bit constant: %d", logicalWidth*8, vec.Min)
	}
	if w := vec.Desc.RowWidth(); w > 0 {
		for i := 0; i < rows; i++ {
			f.HexBytesln(w, "data[%d] = %d", i, f.PeekInt(w))
		}
	}
}
