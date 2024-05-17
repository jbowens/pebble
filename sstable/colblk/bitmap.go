// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"math/bits"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// Bitmap is a bitmap structure built on a []uint64. A bitmap utilizes ~1
// physical bit/logical bit (~0.125 bytes/row). The bitmap is encoded into an
// 8-byte aligned array of 64-bit words which is (nRows+63)/64 words in length.
// A summary bitmap is stored after the primary bitmap where each bit in the
// summary bitmap corresponds to 1 word in the primary bitmap. A bit is set in
// the summary bitmap if the corresponding word in the primary bitmap is
// non-zero. The summary bitmap accelerates predecessor and successor
// operations.
type Bitmap struct {
	data  UnsafeRawSlice[uint64]
	total int
}

// Get returns true if the bit at position i is set and false otherwise.
func (b Bitmap) Get(i int) bool {
	return (b.data.At(i>>6 /* i/64 */) & (1 << uint(i%64))) != 0
}

// nextInWord TODO(peter) ...
func (b Bitmap) nextInWord(wordIdx int, bit uint) int {
	word := b.data.At(wordIdx)
	// Only consider bits that are >= the current bit index.
	return 64*wordIdx + bits.TrailingZeros64(word&^((1<<bit)-1))
}

// Successor returns the next bit greater than or equal to i set in the
// bitmap. Returns b.total if no next bit is set.
func (b Bitmap) Successor(i int) int {
	wordIdx := i / 64
	if next := b.nextInWord(wordIdx, uint(i%64)); next/64 == wordIdx {
		if next >= b.total {
			panicf("not reached: next=%d >= total=%d", next, b.total)
		}
		return next
	}

	// Consult summary structure to find the next word to check.
	summaryBase := align(b.total, 64)
	nWords := summaryBase / 64

	// We've already consulted wordIdx, so iterate from wordIdx+1.
	for wordIdx++; wordIdx < nWords; {
		summaryWordIdx := wordIdx / 64
		next := b.nextInWord(nWords+summaryWordIdx, uint(wordIdx%64))
		if next/64 == nWords+summaryWordIdx {
			wordIdx = next - summaryBase
			if wordIdx >= nWords {
				panicf("not reached: wordIdx=%d >= nWords=%d", wordIdx, nWords)
			}
			break
		}

		wordIdx = next
	}

	if wordIdx >= nWords {
		return b.total
	}

	if next := b.nextInWord(wordIdx, 0); next/64 == wordIdx {
		if next >= b.total {
			panicf("not reached: next=%d >= total=%d", next, b.total)
		}
		return next
	}
	panicf("not reached: no bit set in wordIdx=%d", wordIdx)
	return -1
}

// Predecessor returns the previous bit less than or equal to i set in the
// bitmap. Returns -1 if no previous bit is set.
func (b Bitmap) Predecessor(i int) int {
	panic("unimplemented")
}

// bitmapBuilder TODO(peter) ...
type bitmapBuilder []uint64

func bitmapRequiredSize(total int) int {
	nWords := (total + 63) >> 6          // divide by 64
	nSummaryWords := (nWords + 63) >> 6  // divide by 64
	return (nWords + nSummaryWords) << 3 // multiply by 8
}

// Set sets the bit at position i if v is true and clears the bit at position i
// otherwise. Callers need not call Set if v is false and Set(i, true) has not
// been set yet.
//
// Space for the summary bitmap is reserved based on the maximum position i that
// is seen.
func (b bitmapBuilder) Set(i int, v bool) bitmapBuilder {
	w := i >> 6 // divide by 64
	for len(b) <= w {
		b = append(b, 0)
	}
	if v {
		b[w] |= 1 << uint(i%64)
	} else {
		b[w] &^= 1 << uint(i%64)
	}
	return b
}

// Finish finalizes the bitmap, computing the per-word summary bitmap and
// writing the resulting data to buf at offset.
func (b bitmapBuilder) Finish(nRows int, offset uint32, buf []byte) uint32 {
	dest := makeUnsafeRawSlice[uint64](unsafe.Pointer(&buf[offset]))
	offset += uint32(copy(dest.Slice(len(b)), b)) << align64Shift

	// If the tail of b is sparse, fill in zeroes.
	nBitmapWords := (nRows + 63) >> 6
	for i := len(b); i < nBitmapWords; i++ {
		dest.set(i, 0)
		offset += align64
	}

	// Add the summary bitmap.
	nSummaryWords := (nBitmapWords + 63) >> 6
	for i := 0; i < nSummaryWords; i++ {
		wordsOff := (i << 6) // i*64
		nWords := min(64, len(b)-wordsOff)
		var summaryWord uint64
		for j := 0; j < nWords; j++ {
			if b[wordsOff+j] != 0 {
				summaryWord |= 1 << j
			}
		}
		dest.set(nBitmapWords+i, summaryWord)
	}
	offset += uint32(nSummaryWords) << align64Shift
	return offset
}

func bitmapToBinFormatter(f *binfmt.Formatter, rows int) int {
	bitmapWords := (rows + 63) / 64
	for i := 0; i < bitmapWords; i++ {
		f.Line(8).Append("b ").Binary(8).Done("bitmap word %d", i)
	}
	summaryWords := (bitmapWords + 63) >> 6
	for i := 0; i < summaryWords; i++ {
		f.Line(8).Append("b ").Binary(8).Done("bitmap summary word %d-%d", i*64, i*64+63)
	}
	return (bitmapWords + summaryWords) * align64
}

// A sentinel pointer value used to indicate that the NULL bitmap was
// completely populated (i.e. all values for the column were NULL).
var allNullsSentinel = make([]nullBitmapWord, 1)
var allNullsSentinelPtr = unsafe.Pointer(unsafe.SliceData(allNullsSentinel))

// Type definitions and constants to make it easier to change the NullBitmap
// word size. Using a 32-bit word size is slightly faster for Get and Rank
// operations yet imposes a limit of 64K rows in a block.
type nullBitmapWord uint32

const nullBitmapAlign = align32
const nullBitmapAlignShift = align32Shift
const nullBitmapWordSize = 32
const nullBitmapHalfWordSize = nullBitmapWordSize / 2
const nullBitmapHalfWordSizeShift = 4
const nullBitmapHalfWordMask = nullBitmapHalfWordSize - 1

// NullBitmap provides a bitmap for recording the presence of a column value. If
// the i'th bit of the NULL-bitmap for a column is 1, no value is stored for the
// column at that row index. In addition to presence testing, the NULL-bitmap
// provides a fast Rank(i) operation by interleaving a lookup table into the
// bitmap. The rank is the number of non-NULL values present in bitmap[0,i).
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
// The number of bits used for each lookup table entry (16-bits) limits the
// size of a bitmap to 64K bits which limits the number of rows in a block to
// 64K. The lookup table imposes an additional bit of overhead per bit in the
// bitmap (thus 2-bits per row).
type NullBitmap struct {
	data UnsafeRawSlice[nullBitmapWord]
}

func makeNullBitmap(v []nullBitmapWord) NullBitmap {
	return NullBitmap{data: makeUnsafeRawSlice[nullBitmapWord](unsafe.Pointer(unsafe.SliceData(v)))}
}

func makeNullBitmapRaw(ptr unsafe.Pointer) NullBitmap {
	return NullBitmap{data: makeUnsafeRawSlice[nullBitmapWord](ptr)}
}

// Empty returns true if the bitmap is empty and indicates that all of the
// column values are non-NULL. It is safe to call Null and Rank on an empty
// bitmap, but faster to specialize code to not invoke them at all.
func (b NullBitmap) Empty() bool {
	return b.data.ptr == nil
}

// Full returns true if the bitmap is completely populated indicating that all
// of the column values are NULL. It is safe to call Null and Rank on a full
// bitmap, but faster to specialize code to not invoke them at all.
func (b NullBitmap) Full() bool {
	return b.data.ptr == allNullsSentinelPtr
}

// Null returns true if the bit at position i is set and false otherwise.
func (b NullBitmap) Null(i int) bool {
	if b.data.ptr == nil {
		return false
	}
	if b.data.ptr == allNullsSentinelPtr {
		return true
	}
	word := b.data.At(i >> nullBitmapHalfWordSizeShift)
	bit := nullBitmapWord(1) << uint(i&nullBitmapHalfWordMask)
	return (word & bit) != 0
}

// Rank returns the index of the i'th non-NULL value in the value
// array. Returns -1 if the i'th value is NULL. If all values are non-NULL,
// Rank(i) == i. The pattern to iterate over the non-NULL values in a vector
// is:
//
//	vals := vec.Int64()
//	for i := 0; i < vec.N; i++ {
//	  if j := vec.Rank(i); j >= 0 {
//	    v := vals[j]
//	    // process v
//	  }
//	}
func (b NullBitmap) Rank(i int) int {
	if b.data.ptr == nil {
		return i
	}
	if b.data.ptr == allNullsSentinelPtr {
		return -1
	}
	word := b.data.At(i >> nullBitmapHalfWordSizeShift)
	bit := nullBitmapWord(1) << uint(i&nullBitmapHalfWordMask)
	if (word & bit) != 0 {
		return -1
	}
	// The rank of the i'th bit is the total in the lookup table sum (stored in
	// the high 16 bits), plus the number of zeros in the low 16 bits that
	// precede the bit.
	return int(word>>nullBitmapHalfWordSize) + bits.OnesCount16(uint16(^word&(bit-1)))
}

// count returns the count of non-NULL values in the bitmap.
func (b NullBitmap) count(n int) int {
	if b.data.ptr == nil {
		return n
	}
	if b.data.ptr == allNullsSentinelPtr {
		return 0
	}
	word := b.data.At((n - 1) >> nullBitmapHalfWordSizeShift)
	bit := nullBitmapWord(1) << (uint((n-1)&nullBitmapHalfWordMask) + 1)
	return int(word>>nullBitmapHalfWordSize) + bits.OnesCount16(uint16(^word&(bit-1)))
}

func nullBitmapToBinFormatter(f *binfmt.Formatter, rows int) int {
	var n int
	bm := makeNullBitmapRaw(f.Pointer(0))
	f.CommentLine("NULL bitmap %d/%d are NULL", rows-bm.count(rows), rows)
	for i := 0; i <= (rows-1)/nullBitmapHalfWordSize; i++ {
		word := bm.data.At(i)
		f.Line(4).Binary(1).Append(" ").Binary(1).Append(" ").HexBytes(2).
			Done("word %2d: %016b (sum %d)", i, uint16(word), int(word>>nullBitmapHalfWordSize))
		n += nullBitmapWordSize / 8
	}
	return n
}

// nullBitmapBuilder builds a NullBitmap.
type nullBitmapBuilder []nullBitmapWord

// Set sets the bit at position i if v is true and clears the bit at position i
// otherwise. Bits must be set in order and it is invalid to set a bit twice.
func (b nullBitmapBuilder) Set(i int, v bool) nullBitmapBuilder {
	// j is the index of the word containing the i'th bit.
	j := i >> nullBitmapHalfWordSizeShift // i/16

	// Grow the bitmap to the required size, populating the lookup table's high
	// bits as we go.
	for len(b) <= j {
		var p nullBitmapWord
		if len(b) > 0 {
			word := b[len(b)-1]
			p = ((word >> nullBitmapHalfWordSize) + nullBitmapWord(bits.OnesCount16(uint16(^word)))) << nullBitmapHalfWordSize
		}
		b = append(b, p)
	}
	// Set requires that bits be set in order and a bit cannot be set twice. In
	// invariants builds ensure that the bit is not already set, which would
	// indicate a violation of the contract.
	if invariants.Enabled && (b[j]&(nullBitmapWord(1)<<uint(i&nullBitmapHalfWordMask))) != 0 {
		panicf("bit %d already set", i)
	}
	if v {
		// Set the bit within this word.
		b[j] |= nullBitmapWord(1) << uint(i&nullBitmapHalfWordMask)
	}
	return b
}

// Verify validates the integrity of the interleaved lookup table.
func (b nullBitmapBuilder) Verify() {
	if len(b) > 0 {
		if (b[0] >> nullBitmapHalfWordSize) != 0 {
			panicf("nullBitmapBuilder: 0: %08x\n", b[0])
		}
		for i, sum := 1, nullBitmapWord(0); i < len(b); i++ {
			sum += nullBitmapWord(bits.OnesCount16(^uint16(b[i-1])))
			if (b[i] >> nullBitmapHalfWordSize) != sum {
				panicf("nullBitmapBuilder: i: %08x vs %08x\n", b[i], (sum << nullBitmapHalfWordSize))
			}
		}
	}
}

// Size returns the size of the NULL bitmap in bytes, when written at the
// provided offset. The serialized bitmap is stored aligned at a 32-bit offset,
// so the space required for serializing the bitmap is dependent on the current
// offset within a buffer.
func (b nullBitmapBuilder) Size(offset uint32) uint32 {
	offset = align(offset, nullBitmapAlign)
	offset += uint32(len(b)) << nullBitmapAlignShift /* len(b) * nullBitmapAlign */
	return offset
}

func nullBitmapSize(offset uint32, rows int) uint32 {
	return alignPadding(offset, nullBitmapAlign) + uint32((rows+nullBitmapHalfWordSize-1)/nullBitmapHalfWordSize)<<nullBitmapAlignShift
}

// Finish serializes the NULL bitmap into the provided buffer at the provided
// offset. The buffer must be at least Size(offset) bytes in length. Finish does
// not have special casing for full or empty bitmaps, and it's the caller's
// responsibility to special case these scenarios.
func (b nullBitmapBuilder) Finish(offset uint32, buf []byte) uint32 {
	b.Verify()
	offset = alignWithZeroes(buf, offset, nullBitmapAlign)
	dest := makeUnsafeRawSlice[nullBitmapWord](unsafe.Pointer(&buf[offset]))
	copy(dest.Slice(len(b)), b)
	offset += uint32(len(b)) << nullBitmapAlignShift
	return offset
}
