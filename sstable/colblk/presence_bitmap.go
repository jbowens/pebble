// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"io"
	"math/bits"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// presenceEncoding is an enum used for columns that permit values to be absent.
// It encodes whether a column has all values present, all values absent, or
// encodes a presence bitmap indicating which values are present.
type presenceEncoding int8

const (
	// presenceEncodingAllAbsent indicates that all values in the column are
	// absent. A reader does not need to read any further column data.
	presenceEncodingAllAbsent presenceEncoding = 0x00
	// presenceEncodingAllPresent indicates that all values in the column are
	// present. No presence bitmap is encoded, and immediately following the
	// presenceEncoding bit is the column data.
	presenceEncodingAllPresent presenceEncoding = 0x01
	// presenceEncodingSomeAbsent indicates that some but not all values are
	// absent from the column. The column data is prefixed with a presence
	// bitmap that should be read first. The actual column data that follows
	// will only encode as many values as the presence bitmap indicates are
	// present.
	presenceEncodingSomeAbsent presenceEncoding = 0x02
)

// PresenceWithDefault is a ColumnReader used for reading columns that were
// encoded with some values absent, a PresenceBitmap indicating which values are
// present. PresenceWithDefault wraps another ColumnReader. When a value is
// present, it returns the value from the wrapped ColumnReader. When a value is
// absent, it returns configured default value.
type PresenceWithDefault[V any, R ColumnReader[V]] struct {
	PresenceBitmap
	reader       R
	defaultValue V
}

// At implements ColumnReader, returning the row'th value of the column.
func (d PresenceWithDefault[V, R]) At(row int) V {
	idx := d.PresenceBitmap.Rank(row)
	if idx == -1 {
		return d.defaultValue
	}
	return d.reader.At(idx)
}

// DecodePresenceWithDefault returns a DecodeFunc that decodes a column that was
// encoded with potentially absent column values. It returns a
// PresenceWithDefault that includes knowledge of which values are present and a
// default value to use when a value is absent.
func DecodePresenceWithDefault[V any, R ColumnReader[V]](
	defaultValue V, columnDecoder DecodeFunc[R],
) DecodeFunc[PresenceWithDefault[V, R]] {
	return func(buf []byte, offset uint32, rows int) (r PresenceWithDefault[V, R], nextOffset uint32) {
		d := PresenceWithDefault[V, R]{
			defaultValue: defaultValue,
		}
		d.PresenceBitmap, offset = ReadPresence(buf, offset, rows)
		d.reader, offset = columnDecoder(buf, offset, d.PresenceBitmap.PresentCount())
		return d, offset
	}
}

// TODO(jackson): Impose consistent interfaces on the various column decoders
// where possible, and provide more sugar for reading columns prefixed with
// presence. For example, we might have a 'column reader' implementation that's
// generic in the column data type T, takes a default T to assign to absent
// values, and handles the Rank lookup.
//
// It'll be easier to get this right once we have the use cases in place so that
// we can clearly see the common patterns.

// ReadPresence reads the presence encoding from buf at the provided offset. It
// returns a PresenceBitmap that may be used to determine which values are
// present and which are absent. It returns an endOffset pointing to the first
// byte after the presence encoding.
//
// An example of reading all the values in a RawBytes column with a presence
// bitmap, interpreting absence as the empty byte slice.
//
//		presence, offset := ReadPresence(buf, offset, rows)
//		rawByteValues := MakeRawBytes(presence.PresentCount(rows), buf, offset)
//		for i := 0; i < rows; i++ {
//	      var value []byte
//		  if idx := presence.Rank(i); idx != -1 {
//		    value = rawByteValues.At(idx)
//		  }
//		  // process value
//		}
func ReadPresence(buf []byte, offset uint32, rows int) (presence PresenceBitmap, endOffset uint32) {
	// The first byte is an enum indicating how presence is encoded.
	switch presenceEncoding(buf[offset]) {
	case presenceEncodingAllAbsent:
		return PresenceBitmap{ /* nil ptr indicates all absent */ }, offset + 1
	case presenceEncodingAllPresent:
		bitmap := PresenceBitmap{data: makeUnsafeRawSlice[presenceBitmapWord](allPresentSentinelPtr)}
		return bitmap, offset + 1
	case presenceEncodingSomeAbsent:
		bitmap := PresenceBitmap{data: makeUnsafeRawSlice[presenceBitmapWord](unsafe.Pointer(&buf[offset+1]))}
		return bitmap, presenceBitmapSize(offset+1, rows)
	default:
		panic(base.MarkCorruptionError(errors.Newf("unknown presence encoding: %d", buf[offset])))
	}
}

// DefaultAbsent implements ColumnWriter, wrapping another ColumnWriter and
// handling cheaply encoding absent values using a presence bitmap. Rows default
// to absent unless explicitly set through Present.
//
// Columns written with DefaultAbsent prefix the column data with a single byte
// indicating how the data is encoded and possibly a presence bitmap. Readers of
// a column written with DefaultAbsent should use ReadPresence to retrieve a
// PresenceBitmap for the column.
type DefaultAbsent[W ColumnWriter] struct {
	Writer W

	bitmap       presenceBitmapBuilder
	presentCount int
}

// Assert that DefaultAbsent implements ColumnWriter.
var _ ColumnWriter = (*DefaultAbsent[ColumnWriter])(nil)

// Present marks the value at the given row as present and returns the index to
// use for its value within the underlying value array:
//
//	cw, idx = cw.Present(idx)
//	cw.Set(idx, value)
func (n *DefaultAbsent[W]) Present(row int) (W, int) {
	n.bitmap.Set(row, true)
	n.presentCount++
	return n.Writer, n.presentCount - 1
}

// NumColumns implements ColumnWriter.
func (n *DefaultAbsent[W]) NumColumns() int { return n.Writer.NumColumns() }

// DataType implements ColumnWriter.
func (n *DefaultAbsent[W]) DataType(col int) DataType { return n.Writer.DataType(col) }

// Reset implements ColumnWriter, resetting the column (and its underlying
// ColumnWriter) to its empty state.
func (n *DefaultAbsent[W]) Reset() {
	clear(n.bitmap.words)
	n.bitmap.words = n.bitmap.words[:0]
	n.presentCount = 0
	n.Writer.Reset()
}

// Size implements ColumnWriter.
func (n *DefaultAbsent[W]) Size(rows int, offset uint32) uint32 {
	offset++ // the presenceEncoding enum byte
	switch n.presentCount {
	case 0:
		// All absent; doesn't need to be encoded into column data.
		return offset
	case rows:
		// All values are present.
		return n.Writer.Size(n.presentCount, offset)
	default:
		// Mix of present and absent values. We need a bitmap to encode which
		// values are absent.
		return n.Writer.Size(n.presentCount, n.bitmap.Size(offset))
	}
}

// Finish implemnets ColumnWriter.
func (n *DefaultAbsent[W]) Finish(col, rows int, offset uint32, buf []byte) (endOffset uint32) {
	// NB: The struct field n.presentCount may have been computed for a number
	// of rows higher than [rows]. Use the bitmap's lookup table to compute the
	// number of present rows up to [rows].
	switch n.bitmap.PresentCount(rows) {
	case 0:
		buf[offset] = byte(presenceEncodingAllAbsent)
		return offset + 1
	case rows:
		buf[offset] = byte(presenceEncodingAllPresent)
		return n.Writer.Finish(col, n.presentCount, offset+1, buf)
	default:
		buf[offset] = byte(presenceEncodingSomeAbsent)
		offset = n.bitmap.Finish(offset+1, buf)
		offset = n.Writer.Finish(col, n.presentCount, offset, buf)
		return offset
	}
}

// WriteDebug implements ColumnWriter.
func (n *DefaultAbsent[W]) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "defaultabsent[%d present](", n.presentCount)
	n.Writer.WriteDebug(w, n.presentCount)
	fmt.Fprintf(w, ")")
}

// Type definitions and constants to make it easier to change the PresenceBitmap
// word size. Using a 32-bit word size is slightly faster for Get and Rank
// operations yet imposes a limit of 64K rows in a block.
type presenceBitmapWord uint32

const presenceBitmapAlign = align32
const presenceBitmapAlignShift = align32Shift
const presenceBitmapWordSize = 32
const presenceBitmapHalfWordSize = presenceBitmapWordSize / 2
const presenceBitmapHalfWordSizeShift = 4
const presenceBitmapHalfWordMask = presenceBitmapHalfWordSize - 1

// A sentinel pointer value used to indicate that the presence bitmap was
// completely empty (i.e. all values for the column were absent).
var allPresentSentinel = []presenceBitmapWord{presenceBitmapWord(0xFFFFFFFF)}
var allPresentSentinelPtr = unsafe.Pointer(unsafe.SliceData(allPresentSentinel))

// PresenceBitmap provides a bitmap for recording the presence of a column
// value. If the i'th bit of the presence bitmap for a column is 1, a value is
// stored for the column at that row index. In addition to presence testing, the
// presence-bitmap provides a fast Rank(i) operation by interleaving a lookup
// table into the bitmap. The rank is the number of present values present in
// bitmap[0,i).
//
// The meaning of an absence value is column-specific. A column may encode a
// PresenceBitmap assigning a "zero value" to the absence of a value.
//
// The bitmap is organized as an array of 32-bit words where the bitmap is
// stored in the low 16-bits of every 32-bit word and the lookup table is stored
// in the high bits.
//
//	 bits    sum    bits    sum     bits    sum     bits    sum
//	+-------+------+-------+-------+-------+-------+-------+-------+
//	| 0-15  | 0    | 16-31 | 0-15  | 32-47 | 0-31  | 48-64 | 0-63  |
//	+-------+------+-------+-------+-------+-------+-------+-------+
//
// For example, consider the following 64-bits of data:
//
//	1110011111011111 1101111011110011 1111111111111111 1111110000111111
//
// The logical bits are split at 16-bit boundaries
//
//	       bits             sum
//	0-15:  1110011111011111 0
//	16-31: 1101111011110011 13
//	32-47: 1111111111111111 25
//	48-63: 1111110000011111 41
//
// The lookup table (the sum column) is interleaved with the bitmap in the high
// 16 bits. To answer a Rank query, we find the word containing the bit (i/16),
// count the number of bits that are set in the low 16 bits of the word before
// the bit we're interested in, and add the sum from the high 16 bits in the
// word.
//
// The number of bits used for each lookup table entry (16-bits) limits the size
// of a bitmap to 64K bits which limits the number of rows in a block to 64K.
// The lookup table imposes an additional bit of overhead per bit in the bitmap
// (thus 2-bits per row).
type PresenceBitmap struct {
	rows int
	data UnsafeRawSlice[presenceBitmapWord]
}

func makePresenceBitmap(rows int, v []presenceBitmapWord) PresenceBitmap {
	return PresenceBitmap{
		rows: rows,
		data: makeUnsafeRawSlice[presenceBitmapWord](unsafe.Pointer(unsafe.SliceData(v))),
	}
}

func makePresenceBitmapRaw(rows int, ptr unsafe.Pointer) PresenceBitmap {
	return PresenceBitmap{
		rows: rows,
		data: makeUnsafeRawSlice[presenceBitmapWord](ptr),
	}
}

// AllPresent returns true if the bitmap is empty and indicates that all of the
// column values are present. It is safe to call Null and Rank on such a bitmap,
// but faster to specialize code to not invoke them at all.
func (b PresenceBitmap) AllPresent() bool {
	return b.data.ptr == allPresentSentinelPtr
}

// AllAbsent returns true if the bitmap indicates that all of the column values
// are absent. It is safe to call Null and Rank on a full bitmap, but faster to
// specialize code to not invoke them at all.
func (b PresenceBitmap) AllAbsent() bool {
	return b.data.ptr == nil
}

// Present returns true if the bit at position i is set, indicating a value is
// present, and false otherwise.
func (b PresenceBitmap) Present(i int) bool {
	if b.data.ptr == nil {
		return false
	}
	if b.data.ptr == allPresentSentinelPtr {
		return false
	}
	word := b.data.At(i >> presenceBitmapHalfWordSizeShift)
	bit := presenceBitmapWord(1) << uint(i&presenceBitmapHalfWordMask)
	return (word & bit) != 0
}

// Rank looks up the existence of the i'th row. If the i'th row's value is
// present, it returns the index of the value in the value array. Rank returns
// -1 if the i'th value is absent. If all values are present, Rank(i) == i.
func (b PresenceBitmap) Rank(i int) int {
	if b.data.ptr == nil {
		return -1
	}
	if b.data.ptr == allPresentSentinelPtr {
		return i
	}
	word := b.data.At(i >> presenceBitmapHalfWordSizeShift)
	bit := presenceBitmapWord(1) << uint(i&presenceBitmapHalfWordMask)
	if (word & bit) == 0 {
		// The i'th value is not present.
		return -1
	}
	// The rank of the i'th bit is the total in the lookup table sum (stored in
	// the high 16 bits), plus the number of ones in the low 16 bits that
	// precede the bit.
	v := int(word>>presenceBitmapHalfWordSize) + bits.OnesCount16(uint16(word&(bit-1)))
	return v
}

// PresentCount returns the number of values that are recorded as present.
func (b PresenceBitmap) PresentCount() int {
	if b.data.ptr == nil {
		return 0
	}
	if b.data.ptr == allPresentSentinelPtr {
		return b.rows
	}
	word := b.data.At(b.rows - 1>>presenceBitmapHalfWordSizeShift)
	bit := presenceBitmapWord(1) << uint(b.rows-1&presenceBitmapHalfWordMask)
	return int(word>>presenceBitmapHalfWordSize) + bits.OnesCount16(uint16(word&(bit-1)))
}

func presenceBitmapToBinFormatter(f *binfmt.Formatter, rows int) int {
	var n int
	bm := makePresenceBitmapRaw(rows, f.Pointer(0))
	for i := 0; i <= (rows-1)/presenceBitmapHalfWordSize; i++ {
		word := bm.data.At(i)
		f.Line(4).Binary(1).Append(" ").Binary(1).Append(" ").HexBytes(2).
			Done("word %2d: %016b (sum %d)", i, uint16(word), int(word>>presenceBitmapHalfWordSize))
		n += presenceBitmapWordSize / 8
	}
	return n
}

// presenceBitmapBuilder builds a PresenceBitmap.
type presenceBitmapBuilder struct {
	words []presenceBitmapWord
}

// Set sets the bit at position i if v is true and clears the bit at position i
// otherwise. Bits must be set in order and it is invalid to set a bit twice.
func (b *presenceBitmapBuilder) Set(i int, v bool) {
	// j is the index of the word containing the i'th bit.
	j := i >> presenceBitmapHalfWordSizeShift // i/16

	// Grow the bitmap to the required size, populating the lookup table's high
	// bits as we go.
	for len(b.words) <= j {
		var p presenceBitmapWord
		if len(b.words) > 0 {
			word := b.words[len(b.words)-1]
			p = ((word >> presenceBitmapHalfWordSize) + presenceBitmapWord(bits.OnesCount16(uint16(word)))) << presenceBitmapHalfWordSize
		}
		b.words = append(b.words, p)
	}
	// Set requires that bits be set in order and a bit cannot be set twice. In
	// invariants builds ensure that the bit is not already set, which would
	// indicate a violation of the contract.
	if invariants.Enabled && (b.words[j]&(presenceBitmapWord(1)<<uint(i&presenceBitmapHalfWordMask))) != 0 {
		panic(errors.AssertionFailedf("bit %d already set", i))
	}
	if v {
		// Set the bit within this word.
		b.words[j] |= presenceBitmapWord(1) << uint(i&presenceBitmapHalfWordMask)
	}
}

// PresentCount returns the number of present values in the bitmap among the
// first n rows.
func (b presenceBitmapBuilder) PresentCount(n int) int {
	if len(b.words) == 0 {
		return 0
	}
	w := min((n-1)>>presenceBitmapHalfWordSizeShift, len(b.words)-1)
	word := b.words[w]
	bit := presenceBitmapWord(1) << (uint((n-1)&presenceBitmapHalfWordMask) + 1)
	return n - (int(word>>presenceBitmapHalfWordSize) + bits.OnesCount16(uint16(word&(bit-1))))
}

// verify validates the integrity of the interleaved lookup table.
func (b presenceBitmapBuilder) verify() {
	if len(b.words) > 0 {
		if (b.words[0] >> presenceBitmapHalfWordSize) != 0 {
			panic(errors.AssertionFailedf("presenceBitmapBuilder: 0: %08x", b.words[0]))
		}
		for i, sum := 1, presenceBitmapWord(0); i < len(b.words); i++ {
			sum += presenceBitmapWord(bits.OnesCount16(uint16(b.words[i-1])))
			if (b.words[i] >> presenceBitmapHalfWordSize) != sum {
				panic(errors.AssertionFailedf("presenceBitmapBuilder: i: %08x vs %08x", b.words[i], sum<<presenceBitmapHalfWordSize))
			}
		}
	}
}

// Size returns the size of the presence bitmap in bytes, when written at the
// provided offset. The serialized bitmap is stored aligned at a 32-bit offset,
// so the space required for serializing the bitmap is dependent on the current
// offset within a buffer.
//
// Size does not have special casing for full or empty bitmaps, and it's the
// responsibility of the caller to special case those cases if necessary.
func (b presenceBitmapBuilder) Size(offset uint32) uint32 {
	offset = align(offset, presenceBitmapAlign)
	offset += uint32(len(b.words)) << presenceBitmapAlignShift /* len(b) * presenceBitmapAlign */
	return offset
}

func presenceBitmapSize(offset uint32, rows int) uint32 {
	return align(offset, presenceBitmapAlign) +
		uint32((rows+presenceBitmapHalfWordSize-1)/presenceBitmapHalfWordSize)<<presenceBitmapAlignShift
}

// Finish serializes the NULL bitmap into the provided buffer at the provided
// offset. The buffer must be at least Size(offset) bytes in length.
//
//	Finish does not have special casing for full or empty bitmaps, and it's the
//	caller's responsibility to special case these scenarios.
func (b presenceBitmapBuilder) Finish(offset uint32, buf []byte) uint32 {
	b.verify()
	offset = alignWithZeroes(buf, offset, presenceBitmapAlign)
	dest := makeUnsafeRawSlice[presenceBitmapWord](unsafe.Pointer(&buf[offset]))
	copy(dest.Slice(len(b.words)), b.words)
	offset += uint32(len(b.words)) << presenceBitmapAlignShift
	return offset
}
