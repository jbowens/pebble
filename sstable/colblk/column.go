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
	"math/bits"
	"strings"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
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
	// Finish serializes the column's values into the provided buffer at the
	// provided offset, returning the new offset and the column's descriptor.
	//
	// The `rows` argument must be the current number of logical rows in the
	// column.  Some implementations support defaults, and these implementations
	// rely on the caller to inform them the current number of logical rows.
	Finish(rows int, offset uint32, buf []byte) (uint32, ColumnDesc)
}

type MultiColumnWriter interface {
	Encoder
	// NumColumns returns the number of columns the MultiColumnWriter will encode.
	NumColumns() int
	// Finish serializes the column at the specified index, writing the column's
	// data to buf at offset, and returning the offset at which the next column
	// should be encoded. Finish also returns a column descriptor describing the
	// encoding of the column, which will be serialized within the block header.
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
	// rely on the caller to inform them the current number of logical rows.
	Size(rows int, offset uint32) uint32
	// WriteDebug writes a human-readable description of the current column
	// state to the provided writer.
	WriteDebug(w io.Writer, rows int)
}

// PrefixBytes holds an array of lexicograpghically ordered byte slices. It
// provides prefix compression. Prefix compression applies strongly to two cases
// in CockroachDB: removal of the "[/tenantID]/tableID/indexID" prefix that is
// present on all table data keys, and multiple versions of a key that are
// distinguished only by different timestamp suffixes. With columnar blocks
// enabling the timestamp to be placed in a separate column, the multiple
// version problem becomes one of efficiently handling exact duplicate keys.
// PrefixBytes builds offs of the RawBytes encoding, introducing n/bundleSize+1
// additional slices for encoding n/bundleSize bundle prefixes and 1
// column-level prefix.
//
// To understand the PrefixBytes layout, we'll work through an example using
// these 15 keys:
//
//	   01234567890
//	 0 aaabbbc
//	 1 aaabbbcc
//	 2 aaabbbcde
//	 3 aaabbbce
//	 4 aaabbbdee*
//	 5 aaabbbdee*
//	 6 aaabbbdee*
//	 7 aaabbbeff
//	 8 aaabbe
//	 9 aaabbeef*
//	10 aaabbeef*
//	11 aaabc
//	12 aabcceef*
//	13 aabcceef*
//	14 aabcceef*
//
// The total length of these keys is 127 bytes. There are 3 keys which occur
// multiple times (delineated by the * suffix) which models multiple versions of
// the same MVCC key in CockroachDB. There is a shared prefix to all of the keys
// which models the "[/tenantID]/tableID/indexID" present on CockroachDB table
// data keys. There are other shared prefixes which model identical values in
// table key columns.
//
// The table below shows the components of the KeyBytes encoding for these 15
// keys when using a bundle size of 4 which results in 4 bundles. The 15 keys
// are encoded into 20 slices: 1 block prefix, 4 bundle prefixes, and 15
// suffixes.  The first slice in the table is the block prefix that is shared by
// all keys in the block. The first slice in each bundle is the bundle prefix
// which is shared by all keys in the bundle.
//
//	 idx   | row   | offset | data
//	-------+-------+--------+------
//	     0 |       |      2 | aa
//	     1 |       |      7 | ..abbbc
//	     2 |     0 |      7 | .......
//	     3 |     1 |      8 | .......c
//	     4 |     2 |     10 | .......de
//	     5 |     3 |     11 | .......e
//	     6 |       |     15 | ..abbb
//	     7 |     4 |     19 | ......dee*
//	     8 |     5 |     19 | ......
//	     9 |     6 |     19 | ......
//	    10 |     7 |     22 | ......eff
//	    11 |       |     24 | ..ab
//	    12 |     8 |     26 | ....be
//	    13 |     9 |     31 | ....beef*
//	    14 |    10 |     31 | ....
//	    15 |    11 |     32 | ....c
//	    16 |       |     39 | ..bcceef*
//	    17 |    12 |     39 | .........
//	    18 |    13 |     39 | .........
//	    19 |    14 |     39 | .........
//
// The offsets column in the table points to the start and end index within the
// RawBytes data array for each of the 20 slices defined above (the 15 key
// suffixes + 4 bundle key prefixes + block key prefix). Offset[0] is the length
// of the first slice which is always anchored at data[0]. The data columns
// display the portion of the data array the slice covers. For row slices, an
// empty suffix column indicates that the slice is identical to the slice at the
// previous index which is indicated by the slice's offset being equal to the
// previous slice's offset. Due to the lexicographic sorting, the key at row i
// can't be a prefix of the key at row i-1 or it would have sorted before the
// key at row i-1. And if the key differs then only the differing bytes will be
// part of the suffix and not contained in the bundle prefix.
//
// The end result of this encoding is that we can store the 127 bytes of the 15
// keys plus their start and end offsets (which would naively consume 15*4=60
// bytes for at least the key lengths) in 83 bytes (39 bytes of data + 42 bytes
// of offset data + 1 byte of bundle size). This encoding provides O(1) access to
// any row by calculating the bundle for the row (5*(row/4)), then the row's
// index within the bundle (1+(row%4)). If the slice's offset equals the previous
// slice's offset then we step backward until we find a non-empty slice or the
// start of the bundle (a variable number of steps, but bounded by the bundle
// size). Forward iteration can easily reuse the previous row's key with a check
// on whether the row's slice is empty. Reverse iteration can reuse the next
// row's key by looking at the next row's offset to determine whether we are in
// the middle of a run of equal keys or at an edge. When reverse iteration steps
// over an edge it has to continue backward until a non-empty slice is found
// (just as in absolute positioning). The Seek{GE,LT} routines first binary
// search on the first key of each bundle which can be retrieved without data
// movement because the bundle prefix is immediately adjacent to it in the data
// array. We can slightly optimize the binary search by skipping over all of the
// keys in the bundle on prefix mismatches.
type PrefixBytes struct {
	rows        int
	bundleShift int
	bundleMask  int
	rawBytes    RawBytes
}

// makePrefixBytes TODO(peter) ...
func makePrefixBytes(count uint32, start unsafe.Pointer) PrefixBytes {
	// The first byte of a PrefixBytes-encoded column is the bundle size
	// expressed as log2 of the bundle size (the bundle size must always be a
	// power of two).
	bundleShift := int(*((*uint8)(unsafe.Pointer(uintptr(start)))))
	bundleSize := 1 << bundleShift
	nBundles := (count + uint32(bundleSize) - 1) / uint32(bundleSize)

	return PrefixBytes{
		rows:        int(count),
		bundleShift: bundleShift,
		bundleMask:  ^((1 << bundleShift) - 1),
		rawBytes:    makeRawBytes(count+nBundles, unsafe.Pointer(uintptr(start)+1)),
	}
}

// rowIndex TODO(peter) ...
func (b PrefixBytes) rowIndex(row int) int {
	return 2 + (row >> b.bundleShift) + row
}

// SharedPrefix return a []byte of the shared prefix that was extracted from
// all of the values in the Bytes vector. The returned slice should not be
// mutated.
func (b PrefixBytes) SharedPrefix() []byte {
	// The very first slice is the prefix for the entire column.
	return b.rawBytes.slice(0, b.rawBytes.offset(0))
}

// BundlePrefix returns a []byte of the prefix shared among all the keys in the
// row's bundle. The returned slice should not be mutated.
func (b PrefixBytes) BundlePrefix(row int) []byte {
	// AND-ing the row with the bundle mask removes the least significant bits
	// of the row, which encode the row's index within the bundle.
	i := (row >> b.bundleShift) + (row & b.bundleMask)
	return b.rawBytes.slice(b.rawBytes.offset(i), b.rawBytes.offset(i+1))
}

// RowSuffix returns a []byte of the suffix unique to the row. A row's full key
// is the result of concatenating SharedPrefix(), BundlePrefix() and
// RowSuffix().
//
// The returned slice should not be mutated.
func (b PrefixBytes) RowSuffix(row int) []byte {
	i := 1 + (row >> b.bundleShift) + row
	// Retrieve the low and high offsets indicating the start and end of the
	// row's suffix slice.
	lowOff := b.rawBytes.offset(i)
	highOff := b.rawBytes.offset(i + 1)
	// If there's a non-empty slice for the row, this row is different than its
	// predecessor.
	if lowOff != highOff {
		return b.rawBytes.slice(lowOff, highOff)
	}
	// Otherwise, an empty slice indicates a duplicate key. We need to find the
	// first non-empty predecessor within the bundle, or if all the rows are
	// empty, return nil.
	//
	// Compute the index of the first row in the bundle so we know when to stop.
	firstIndex := 1 + (row >> b.bundleShift) + (row & b.bundleMask)
	for i > firstIndex {
		// Step back a row, and check if the slice is non-empty.
		i--
		highOff = lowOff
		lowOff = b.rawBytes.offset(i)
		if lowOff != highOff {
			return b.rawBytes.slice(lowOff, highOff)
		}
	}
	// All the rows in the bundle are empty.
	return nil
}

// Rows returns the count of rows whose keys are encoded within the PrefixBytes.
func (b PrefixBytes) Rows() int {
	return b.rows
}

// DebugString returns a human-readable string representation of the PrefixBytes
// internal structure.
func (b PrefixBytes) DebugString() string {
	var buf strings.Builder
	blockPrefix := strings.Repeat(".", b.rawBytes.offset(0))
	var bundlePrefix string
	for i := 0; i < 1+b.rawBytes.Slices(); i++ {
		if i == 0 {
			fmt.Fprintf(&buf, " %-5s | %-5s | %-5s | %s\n",
				"idx", "row", "offset", "data")
			fmt.Fprintf(&buf, "-------+-------+--------+------\n")
			fmt.Fprintf(&buf, " %5d |       | %6d | %s\n",
				i, b.rawBytes.offset(i), b.rawBytes.slice(0, b.rawBytes.offset(i)))
		} else if (i-1)%(1+(1<<b.bundleShift)) == 0 {
			bundlePrefix = strings.Repeat(".", b.rawBytes.offset(i)-b.rawBytes.offset(i-1))
			fmt.Fprintf(&buf, " %5d |       | %6d | %s%s\n",
				i, b.rawBytes.offset(i), blockPrefix,
				b.rawBytes.slice(b.rawBytes.offset(i-1), b.rawBytes.offset(i)))
		} else {
			row := (i - 2) - (i-1)/(1+(1<<b.bundleShift))
			fmt.Fprintf(&buf, " %5d | %5d | %6d | %s%s%s\n",
				i, row, b.rawBytes.offset(i), blockPrefix, bundlePrefix,
				b.rawBytes.slice(b.rawBytes.offset(i-1), b.rawBytes.offset(i)))
		}
	}
	return buf.String()
}

// RawBytes holds an array of byte slices, stored as a concatenated data section
// and a series of offsets for each slice. Byte slices within RawBytes are
// stored in their entirety without any compression, ensuring stability without
// copying.
//
// See PrefixBytes for an array of byte slices encoded with prefix compression.
//
// # Representation
//
// An array of N byte slices encodes N+1 offsets. The beginning of the data
// representation holds an offsets table, broken into two sections: 16-bit
// offsets and 32-bit offsets. The first two bytes of the offset table hold the
// count of 16-bit offsets. After the offsets table, the data section encodes
// raw byte data from all slices with no delimiters.
//
//	+----------+--------------------------------------------------------+
//	| uint16 k |                   (aligning padding)                   |
//	+----------+--------------------------------------------------------+
//	|                16-bit offsets (k) [16-bit aligned]                |
//	|                                                                   |
//	| off_0 | off_1 | off_2 | .................................| off_k  |
//	+-------------------------------------------------------------------+
//	|               32-bit offsets (n-k) [32-bit aligned]               |
//	|                                                                   |
//	|   off_{k+1}   |   off_{k+2}      | ................|   off_n      |
//	+-------------------------------------------------------------------+
//	|                           String Data                             |
//	|  abcabcada....                                                    |
//	+-------------------------------------------------------------------+
type RawBytes struct {
	slices     int
	nOffsets16 int
	start      unsafe.Pointer
	data       unsafe.Pointer
	offsets    unsafe.Pointer
}

// makeRawBytes constructs an accessor for an array of byte slices constructed
// by bytesBuilder. Count must be the number of byte slices within the array.
func makeRawBytes(count uint32, start unsafe.Pointer) RawBytes {
	nOffsets := 1 + count
	nOffsets16 := uint32(binary.LittleEndian.Uint16(unsafe.Slice((*byte)(start), 2)))
	nOffsets32 := nOffsets - nOffsets16

	offsets16 := uintptr(start) + align16 /* nOffsets16 */
	offsets16 = align(offsets16, align16)
	offsets16 -= uintptr(start)

	var data uintptr
	if nOffsets32 == 0 {
		// The variable width data resides immediately after the 16-bit offsets.
		data = offsets16 + uintptr(nOffsets16)<<align16Shift
	} else {
		// nOffsets32 > 0
		//
		// The 16-bit offsets must be aligned on a 16-bit boundary, and the
		// 32-bit offsets which immediately follow them must be aligned on a
		// 32-bit boundary. During construction, the bytesBuilder will ensure
		// correct alignment, inserting padding between the start (where the
		// count of 16-bit offsets is encoded) and the beginning of the 16-bit
		// offset table.
		//
		// At read time, we infer the appropriate padding by finding the end of
		// the 16-bit offsets and ensuring it is aligned on a 32-bit boundary,
		// then jumping back from there to the start of the 16-bit offsets.
		offsets32 := offsets16 + uintptr(nOffsets16)<<align16Shift + uintptr(start)
		offsets32 = align(offsets32, align32)
		offsets16 = offsets32 - uintptr(nOffsets16)<<align16Shift - uintptr(start)
		// The variable width data resides immediately after the 32-bit offsets.
		data = offsets32 + uintptr(nOffsets32)<<align32Shift - uintptr(start)
	}

	return RawBytes{
		slices:     int(count),
		nOffsets16: int(nOffsets16),
		start:      start,
		data:       unsafe.Pointer(uintptr(start) + data),
		offsets:    unsafe.Pointer(uintptr(start) + offsets16),
	}
}

func defaultSliceFormatter(x []byte) string {
	return string(x)
}

func rawBytesToBinFormatter(
	f *binfmt.Formatter, count uint32, sliceFormatter func([]byte) string,
) int {
	if sliceFormatter == nil {
		sliceFormatter = defaultSliceFormatter
	}
	rb := makeRawBytes(count, unsafe.Pointer(f.Pointer(0)))
	dataSectionStartOffset := f.Offset() + int(uintptr(rb.data)-uintptr(rb.start))

	var n int
	f.CommentLine("RawBytes")
	n += f.HexBytesln(2, "16-bit offset count: %d", rb.nOffsets16)
	if off := uintptr(f.Pointer(0)); off < uintptr(rb.offsets) {
		n += f.HexBytesln(int(uintptr(rb.offsets)-off), "padding to align offsets table")
	}
	for i := 0; i < rb.nOffsets16; i++ {
		n += f.HexBytesln(2, "off[%d]: %d [overall %d]", i, rb.offset(i), dataSectionStartOffset+rb.offset(i))
	}
	for i := 0; i < int(count+1)-rb.nOffsets16; i++ {
		n += f.HexBytesln(4, "off[%d]: %d [overall %d]", i+rb.nOffsets16, rb.offset(i+rb.nOffsets16), dataSectionStartOffset+rb.offset(i))
	}
	for i := 0; i < rb.slices; i++ {
		s := rb.At(i)
		n += f.HexBytesln(len(s), "data[%d]: %s", i, sliceFormatter(s))
	}
	return n
}

func (b RawBytes) ptr(offset int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(b.data) + uintptr(offset))
}

func (b RawBytes) slice(start, end int) []byte {
	return unsafe.Slice((*byte)(b.ptr(start)), end-start)
}

// offset TODO(peter) ...
func (b RawBytes) offset(i int) int {
	// The <= implies that offset 0 always fits in a 16-bit offset. That
	// offset holds the length of the shared prefix which is capped to fit
	// into a uint16.
	if i <= b.nOffsets16 {
		return int(*(*uint16)(unsafe.Pointer(uintptr(b.offsets) + uintptr(i)<<align16Shift)))
	}
	return int(*(*uint32)(unsafe.Pointer(uintptr(b.offsets) +
		uintptr(b.nOffsets16)<<align16Shift + uintptr(i-b.nOffsets16)<<align32Shift)))
}

// At returns the []byte at index i. The returned slice should not be mutated.
func (b RawBytes) At(i int) []byte {
	s := b.slice(b.offset(i), b.offset(i+1))
	return s
}

// Slices returns the number of []byte slices encoded within the RawBytes.
func (b RawBytes) Slices() int {
	return b.slices
}

// bytesBuilder encodes a column of byte slices. If bundleSize is nonzero,
// bytesBuilder performs prefix compression, reducing the encoded block size.
//
// TODO(jackson): If prefix compression is very effective, the encoded size may
// remain very small while the physical size of the in-progress data slice may
// grow very large. This may pose memory usage problems during block building.
//
// TODO(jackson): Finish
type bytesBuilder struct {
	data                      []byte
	nKeys                     int    // The number of keys added to the builder
	nBundles                  int    // The number of bundles added to the builder
	bundleSize                int    // The number of keys per bundle
	bundleShift               int    // log2(bundleSize)
	completedBundleLen        int    // The encoded size of completed bundles
	currentBundleLen          int    // The raw size of the current bundle's keys
	currentBundleKeys         int    // The number of physical keys in the current bundle
	currentBundlePrefixOffset int    // The index of the offset for the current bundle prefix
	blockPrefixLen            uint32 // The length of the block-level prefix
	lastKeyLen                int    // The length of the last key added to the builder
	offsets                   []uint32
	nOffsets16                uint16
	maxShared                 uint16
	maxOffset16               uint32 // configurable for testing purposes
}

func (b *bytesBuilder) Init(bundleSize int) {
	if bundleSize > 0 && (bundleSize&(bundleSize-1)) != 0 {
		panicf("prefixbytes bundle size %d is not a power of 2", bundleSize)
	}
	*b = bytesBuilder{
		data:        b.data[:0],
		bundleSize:  bundleSize,
		offsets:     b.offsets[:0],
		maxOffset16: (1 << 16) - 1,
	}
	if b.bundleSize > 0 {
		b.bundleShift = bits.TrailingZeros32(uint32(bundleSize))
		b.maxShared = (1 << 16) - 1
	}
}

// Reset TODO(peter) ...
func (b *bytesBuilder) Reset() {
	*b = bytesBuilder{
		data:        b.data[:0],
		bundleSize:  b.bundleSize,
		bundleShift: b.bundleShift,
		offsets:     b.offsets[:0],
		maxOffset16: (1 << 16) - 1,
		maxShared:   b.maxShared,
	}
}

func bytesSharedPrefix(a, b []byte) int {
	asUint64 := func(data []byte, i int) uint64 {
		return binary.LittleEndian.Uint64(data[i:])
	}
	var shared int
	n := min(len(a), len(b))
	for shared < n-7 && asUint64(a, shared) == asUint64(b, shared) {
		shared += 8
	}
	for shared < n && a[shared] == b[shared] {
		shared++
	}
	return shared
}

// PutOrdered ... TODO(jackson)
func (b *bytesBuilder) PutOrdered(key []byte) (samePrefix bool) {
	if invariants.Enabled {
		if b.maxShared == 0 {
			panicf("maxShared must be positive")
		}
	}
	// This is PrefixBytes mode (eg, with prefix compression).

	var bytesSharedWithPrev int
	switch {
	case b.nKeys == 0:
		// We're adding the first key to the block. Initialize the
		// block prefix to the length of this key.
		b.blockPrefixLen = uint32(min(len(key), int(b.maxShared)))
		// Add a placeholder offset for the block prefix length.
		b.offsets = append(b.offsets, 0)
		if b.blockPrefixLen <= b.maxOffset16 {
			b.nOffsets16++
		}
		// Add an offset for the bundle prefix length.
		b.addOffset(uint32(len(b.data) + min(len(key), int(b.maxShared))))
		b.nKeys++
		b.nBundles++
		b.lastKeyLen = len(key)
		b.currentBundleLen = b.lastKeyLen
		b.currentBundlePrefixOffset = 1
		b.currentBundleKeys = 1
		b.data = append(b.data, key...)
		b.addOffset(uint32(len(b.data)))
		return false
	case b.nKeys%b.bundleSize == 0:
		// We're starting a new bundle so we can compute what the
		// encoded size of the previous bundle will be.
		bundlePrefixLen := b.offsets[b.currentBundlePrefixOffset] - b.offsets[b.currentBundlePrefixOffset-1]
		b.completedBundleLen += b.currentBundleLen - (b.currentBundleKeys-1)*int(bundlePrefixLen)

		// Update the block prefix length if necessary. We need to determine
		// whether or not this key equals the previous regardless (the caller
		// depends on us returning a bool indicating as such). We can use this
		// to also determine any change to the block prefix. The block prefix
		// can only shrink if the bytes shared with the previous key are less
		// than the block prefix length, in which case the new block prefix is
		// the number of bytes shared with the previous key.
		bytesSharedWithPrev = bytesSharedPrefix(b.data[len(b.data)-b.lastKeyLen:], key)
		if uint32(bytesSharedWithPrev) < b.blockPrefixLen {
			b.blockPrefixLen = uint32(bytesSharedWithPrev)
		}

		// We're adding the first key to the current bundle. Initialize
		// the bundle prefix to the length of this key.
		b.currentBundlePrefixOffset = len(b.offsets)
		b.addOffset(uint32(len(b.data) + min(len(key), int(b.maxShared))))
		b.nBundles++
		b.nKeys++
		b.lastKeyLen = len(key)
		b.currentBundleLen = b.lastKeyLen
		b.currentBundleKeys = 1
		b.data = append(b.data, key...)
		b.addOffset(uint32(len(b.data)))
		// Because keys are added in lexicographic order, if the previous key
		// has `key` as a prefix, it must be equal to `key`. If not, there would
		// be additional bytes at the end of the previous key, which would imply
		// it should sort after `key` lexicographically.
		return bytesSharedWithPrev == len(key)
	default:
		// Adding a new key to an existing bundle.
		bytesSharedWithPrev = bytesSharedPrefix(b.data[len(b.data)-b.lastKeyLen:], key)
		// Update the bundle prefix length. Note that the shared prefix length
		// can only shrink as new values are added. During construction, the
		// bundle prefix value is stored contiguously in the data array so even
		// if the bundle prefix length changes no adjustment is needed to that
		// value or to the first key in the bundle.
		bundlePrefixLen := b.offsets[b.currentBundlePrefixOffset] - b.offsets[b.currentBundlePrefixOffset-1]
		if uint32(bytesSharedWithPrev) < bundlePrefixLen {
			b.setOffset(b.currentBundlePrefixOffset, b.offsets[b.currentBundlePrefixOffset-1]+uint32(bytesSharedWithPrev))
			if uint32(bytesSharedWithPrev) < b.blockPrefixLen {
				b.blockPrefixLen = uint32(bytesSharedWithPrev)
			}
		}
		b.nKeys++
		if bytesSharedWithPrev == len(key) {
			b.addOffset(b.offsets[len(b.offsets)-1])
			return
		}
		b.lastKeyLen = len(key)
		b.currentBundleLen += b.lastKeyLen
		b.currentBundleKeys++
		b.data = append(b.data, key...)
		b.addOffset(uint32(len(b.data)))
		// Because keys are added in lexicographic order, if the previous key
		// has `key` as a prefix, it must be equal to `key`. If not, there would
		// be additional bytes at the end of the previous key, which would imply
		// it should sort after `key` lexicographically.
		return bytesSharedWithPrev == len(key)
	}
}

// Put TODO(peter) ...
func (b *bytesBuilder) Put(key []byte) {
	// This is RawBytes mode (eg, no prefix compression).
	if b.nKeys == 0 {
		// The first row. Initialize the block prefix to 0 in order to
		// streamline the logic in RawBytes.At() to avoid needing a special
		// case for row 0.
		b.addOffset(0)
	}
	b.nKeys++
	b.data = append(b.data, key...)
	b.addOffset(uint32(len(b.data)))
}

// PutConcat TODO(jackson) ...
func (b *bytesBuilder) PutConcat(k1, k2 []byte) {
	// This is RawBytes mode (eg, no prefix compression).
	if b.nKeys == 0 {
		// The first row. Initialize the block prefix to 0 in order to
		// streamline the logic in RawBytes.At() to avoid needing a special
		// case for row 0.
		b.addOffset(0)
	}
	b.nKeys++
	b.data = append(append(b.data, k1...), k2...)
	b.addOffset(uint32(len(b.data)))
}

func (b *bytesBuilder) addOffset(offset uint32) {
	if offset <= b.maxOffset16 {
		b.nOffsets16++
	}
	b.offsets = append(b.offsets, offset)
}

func (b *bytesBuilder) setOffset(i int, offset uint32) {
	if offset <= b.maxOffset16 {
		b.nOffsets16 = max(b.nOffsets16, uint16(i))
	}
	b.offsets[i] = offset
}

// prefixCompressedSize TODO(peter) ...
func (b *bytesBuilder) prefixCompressedSize(dataLen uint32) uint32 {
	// If we haven't added enough offsets to perform prefix compression, then
	// the compressed length is the uncompressed length.
	if len(b.offsets) <= 2 {
		return dataLen
	}

	// Adjust the current bundle length by stripping off the bundle prefix
	// from all but one of the keys in the bundle (which is accounting for
	// storage of the bundle prefix itself).
	bundlePrefixLen := int(b.offsets[b.currentBundlePrefixOffset] - b.offsets[b.currentBundlePrefixOffset-1])
	currentBundleLen := b.currentBundleLen - (b.currentBundleKeys-1)*bundlePrefixLen

	n := b.completedBundleLen + currentBundleLen
	// Adjust the length to account for the block prefix being stripped from
	// every bundle except the first one.
	n -= (b.nBundles - 1) * int(b.blockPrefixLen)
	return uint32(n)
}

// prefixCompress TODO(peter) ...
func (b *bytesBuilder) prefixCompress() {
	// Check whether prefix compression is disabled for this builder, or
	// whether we haven't added enough offsets to perform prefix compression.
	if b.bundleSize == 0 || len(b.offsets) <= 2 {
		return
	}
	b.offsets[0] = b.blockPrefixLen
	destOffset := b.blockPrefixLen
	var lastRowOffset uint32
	var shared uint32

	// All updates to b.offsets are performed /without/ using setOffset in order
	// to keep the encoded size inline with Size(). Some of the recomputed
	// offsets may be small enough that they could now be encoded in 16-bits.

	// Loop over the slices starting at the bundle prefix of the first bundle.
	// If the slice is a bundle prefix, carve off the suffix that excludes the
	// block prefix. Otherwise, carve off the suffix that excludes the block
	// prefix + bundle prefix.
	for i := 1; i < len(b.offsets); i++ {
		var suffix []byte
		if (i-1)%(b.bundleSize+1) == 0 {
			// This is a bundle prefix.
			suffix = b.data[lastRowOffset+b.blockPrefixLen : b.offsets[i]]
			shared = b.blockPrefixLen + uint32(len(suffix))
			// We don't update lastRowOffset here because the bundle prefix
			// was never actually stored in the data array.
		} else {
			// TODO(peter) ...
			if b.offsets[i] == lastRowOffset {
				b.offsets[i] = b.offsets[i-1]
				continue
			}
			suffix = b.data[lastRowOffset+shared : b.offsets[i]]
			// Update lastRowOffset for the next iteration of this loop.
			lastRowOffset = b.offsets[i]
		}

		destOffset += uint32(copy(b.data[destOffset:], suffix))
		b.offsets[i] = destOffset
	}
	b.data = b.data[:destOffset]
}

// Finish writes the serialized byte slices to buf starting at offset. The buf
// slice must be sufficiently large to store the serialized output. The caller
// should use [Size] to size buf appropriately before calling Finish.
//
// TODO(peter) ...
func (b *bytesBuilder) Finish(rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeBytes)
	if b.bundleSize > 0 {
		desc = ColumnDesc(DataTypePrefixBytes)
		// Encode the bundle shift.
		buf[offset] = byte(b.bundleShift)
		offset++

		// Prefix compress the variable width data and update the offsets
		// accordingly.
		b.prefixCompress()
	}

	// Encode the count of 16-bit offsets.
	binary.LittleEndian.PutUint16(buf[offset:], b.nOffsets16)
	offset += align16

	// The offsets for variable width data.
	paddingBegin := offset
	offset = align(offset, align16)
	offsets16Size := uint32(b.nOffsets16) << align16Shift
	nOffsets32 := len(b.offsets) - int(b.nOffsets16)
	if nOffsets32 > 0 {
		// {Prefix,Raw}Bytes requires that the 32-bit offsets are aligned on a
		// 32-bit boundary and the 16-bit offsets which immediately preceed them
		// are aligned on a 16-bit boundary. We do this by finding the end of
		// the 16-bit offsets and ensuring it is aligned on a 32-bit boundary,
		// then jumping back from there to the start of the 16-bit offsets. This
		// may leave unused padding bytes between the first two bytes that
		// encode nOffsets16 and the first 16-bit offset.
		//
		// makeRawBytes applies the same logic in order to infer the beginning of
		// the 16-bit offsets.
		offset32 := offset + offsets16Size
		offset32 = align(offset32, align32)
		offset = offset32 - offsets16Size
	}

	// Zero the padding between the count of 16-bit offsets and the start of the
	// offsets table for determinism.
	//
	// TODO(jackson): It might be faster to unconditionally zero the maximum
	// padding size immediately after we write nOffsets16. We'd need a guarantee
	// that there are at least 2 offsets.
	for i := paddingBegin; i < offset; i++ {
		buf[i] = 0
	}

	dest16 := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset]))
	for i := 0; i < int(b.nOffsets16); i++ {
		if b.offsets[i] > b.maxOffset16 {
			panicf("%d: encoding offset %d as 16-bit, but it exceeds maxOffset16 %d", i, b.offsets[i], b.maxOffset16)
		}
		dest16.set(i, uint16(b.offsets[i]))
	}
	offset += offsets16Size

	if nOffsets32 > 0 {
		if offset != align(offset, align32) {
			panicf("offset not aligned to 32: %d", offset)
		}
		dest32 := makeUnsafeRawSlice[uint32](unsafe.Pointer(&buf[offset]))
		offset += uint32(nOffsets32) << align32Shift
		copy(dest32.Slice(nOffsets32), b.offsets[b.nOffsets16:])
	}
	offset += uint32(copy(buf[offset:], b.data))
	return offset, desc
}

// Size computes the size required to encode the byte slices beginning at the
// provided offset. The offset is required to ensure proper alignment. The
// returned int32 is the offset of the first byte after the end of the encoded
// data. To compute the size in bytes, subtract the [offset] passed into Size
// from the returned offset.
func (b *bytesBuilder) Size(rows int, offset uint32) uint32 {
	// The nOffsets16 count.
	offset += align16

	dataLen := uint32(len(b.data))
	if b.bundleSize > 0 {
		// The bundleSize. This is only encoded for the PrefixBytes column
		// type.
		offset++
		// The prefix compressed variable width data.
		dataLen = b.prefixCompressedSize(dataLen)
	}

	// The 16-bit offsets for variable width data
	nOffsets16 := int(b.nOffsets16)
	offset = align(offset, align16)
	offset += uint32(nOffsets16) << align16Shift

	// The 32-bit offsets for variable width data
	nOffsets32 := len(b.offsets) - nOffsets16
	if nOffsets32 > 0 {
		offset = align(offset, align32)
		offset += uint32(nOffsets32) << align32Shift
	}
	return offset + dataLen
}

func (b *bytesBuilder) WriteDebug(w io.Writer, rows int) {
	if b.bundleSize > 0 {
		fmt.Fprintf(w, "prefixbytes(%d): %d keys", b.bundleSize, b.nKeys)
	} else {
		fmt.Fprintf(w, "bytes: %d slices", b.nKeys)
	}
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

func (n *Nullable[W]) Finish(rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	if n.nullCount == rows {
		return offset, ColumnDesc(n.dataType).WithEncoding(EncodingAllNull)
	} else if n.nullCount > 0 {
		offset = n.nulls.Finish(offset, buf)
		off, desc := n.writer.Finish(rows-n.nullCount, offset, buf)
		return off, desc.WithNullBitmap()
	}
	return n.writer.Finish(rows-n.nullCount, offset, buf)
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

func (n *DefaultNull[W]) Finish(rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	if n.notNullCount == 0 {
		return offset, ColumnDesc(n.dataType).WithEncoding(EncodingAllNull)
	} else if n.notNullCount != rows {
		offset = n.nulls.Finish(offset, buf)
		offset, desc := n.writer.Finish(n.notNullCount, offset, buf)
		return offset, desc.WithNullBitmap()
	}
	return n.writer.Finish(n.notNullCount, offset, buf)
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
	elems         UnsafeRawSlice[uint64]
}

func (b *Uint64Builder) Init(defaultConfig UintDefault) {
	b.defaultConfig = defaultConfig
	b.Reset()
}

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
	if b.minimum > v {
		b.minimum = v
	}
	if b.maximum < v {
		b.maximum = v
	}
	b.elems.set(row, v)
}

func (b *Uint64Builder) Size(rows int, offset uint32) uint32 {
	offset = align(offset, align64)
	delta := b.maximum - b.minimum
	switch {
	case delta == 0:
		return offset + align64
	case delta < (1 << 8):
		return offset + align64 + uint32(rows)
	case delta < (1 << 16):
		return offset + align64 + uint32(rows<<align16Shift)
	case delta < (1 << 32):
		return offset + align64 + uint32(rows<<align32Shift)
	default:
		return offset + uint32(rows<<align64Shift)
	}
}

func (b *Uint64Builder) Finish(rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeUint64)
	offset = alignWithZeroes(buf, offset, align64)
	writeMinimum := func() {
		binary.LittleEndian.PutUint64(buf[offset:], b.minimum)
		offset += align64
	}
	delta := b.maximum - b.minimum
	switch {
	case delta == 0:
		desc = desc.WithEncoding(EncodingConstant)
		writeMinimum()
	case delta < (1 << 8):
		desc = desc.WithEncoding(EncodingDeltaInt8)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset]))
		reduceUints[uint64, uint8](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows)
	case delta < (1 << 16):
		desc = desc.WithEncoding(EncodingDeltaInt16)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset]))
		reduceUints[uint64, uint16](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows << align16Shift)
	case delta < (1 << 32):
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
	elems         UnsafeRawSlice[uint32]
}

func (b *Uint32Builder) Init(defaultConfig UintDefault) {
	b.defaultConfig = defaultConfig
	b.Reset()
}

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
	if b.minimum > v {
		b.minimum = v
	}
	if b.maximum < v {
		b.maximum = v
	}
	b.elems.set(row, v)
}

func (b *Uint32Builder) Size(rows int, offset uint32) uint32 {
	offset = align(offset, align32)
	delta := b.maximum - b.minimum
	switch {
	case delta == 0:
		return offset + align32
	case delta < (1 << 8):
		return offset + align32 + uint32(rows)
	case delta < (1 << 16):
		return offset + align32 + uint32(rows<<align16Shift)
	default:
		return offset + uint32(rows<<align32Shift)
	}
}

func (b *Uint32Builder) Finish(rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeUint32)
	offset = alignWithZeroes(buf, offset, align32)
	writeMinimum := func() {
		binary.LittleEndian.PutUint32(buf[offset:], b.minimum)
		offset += align32
	}
	delta := b.maximum - b.minimum
	switch {
	case delta == 0:
		desc = desc.WithEncoding(EncodingConstant)
		writeMinimum()
	case delta < (1 << 8):
		desc = desc.WithEncoding(EncodingDeltaInt8)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset]))
		reduceUints[uint32, uint8](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows)
	case delta < (1 << 16):
		desc = desc.WithEncoding(EncodingDeltaInt16)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset]))
		reduceUints[uint32, uint16](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows << align16Shift)
	default:
		offset += uint32(copy(buf[offset:], unsafe.Slice((*byte)(b.elems.ptr), rows<<align32Shift)))
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
	elems         UnsafeRawSlice[uint16]
}

func (b *Uint16Builder) Init(defaultConfig UintDefault) {
	b.defaultConfig = defaultConfig
	b.Reset()
}

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
	if b.minimum > v {
		b.minimum = v
	}
	if b.maximum < v {
		b.maximum = v
	}
	b.elems.set(row, v)
}

func (b *Uint16Builder) Size(rows int, offset uint32) uint32 {
	offset = align(offset, align16)
	delta := b.maximum - b.minimum
	switch {
	case delta == 0:
		return offset + align16
	case delta < (1 << 8):
		return offset + align16 + uint32(rows)
	default:
		return offset + uint32(rows<<align16Shift)
	}
}

func (b *Uint16Builder) Finish(rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeUint16)
	offset = alignWithZeroes(buf, offset, align16)
	writeMinimum := func() {
		binary.LittleEndian.PutUint16(buf[offset:], b.minimum)
		offset += align16
	}
	delta := b.maximum - b.minimum
	switch {
	case delta == 0:
		desc = desc.WithEncoding(EncodingConstant)
		writeMinimum()
	case delta < (1 << 8):
		desc = desc.WithEncoding(EncodingDeltaInt8)
		writeMinimum()
		dest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset]))
		reduceUints[uint16, uint8](b.minimum, b.elems.Slice(rows), dest.Slice(rows))
		offset += uint32(rows)
	default:
		offset += uint32(copy(buf[offset:], unsafe.Slice((*byte)(b.elems.ptr), rows<<align16Shift)))
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
	elems         UnsafeRawSlice[uint8]
}

func (b *Uint8Builder) Init(defaultConfig UintDefault) {
	b.defaultConfig = defaultConfig
	b.Reset()
}

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
	if b.minimum > v {
		b.minimum = v
	}
	if b.maximum < v {
		b.maximum = v
	}
	b.elems.set(row, v)
}

func (b *Uint8Builder) Size(rows int, offset uint32) uint32 {
	switch {
	case b.maximum-b.minimum == 0:
		return offset + 1
	default:
		return offset + uint32(rows)
	}
}

func (b *Uint8Builder) Finish(rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc(DataTypeUint8)
	if b.maximum-b.minimum == 0 {
		buf[offset] = b.minimum
		desc = desc.WithEncoding(EncodingConstant)
		offset++
	} else {
		offset += uint32(copy(buf[offset:], unsafe.Slice((*byte)(b.elems.ptr), rows)))
	}
	return offset, desc
}

func (b *Uint8Builder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "uint8: %d rows", rows)
}

func reduceUints[O constraints.Integer, N constraints.Integer](minimum O, values []O, dst []N) {
	for i := 0; i < len(values); i++ {
		dst[i] = N(values[i] - minimum)
	}
}
