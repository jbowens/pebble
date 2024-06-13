// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"strings"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
)

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
	rows            int
	bundleShift     int
	bundleMask      int
	sharedPrefixLen int
	rawBytes        RawBytes
}

// makePrefixBytes TODO(peter) ...
func makePrefixBytes(count uint32, start unsafe.Pointer) PrefixBytes {
	// The first byte of a PrefixBytes-encoded column is the bundle size
	// expressed as log2 of the bundle size (the bundle size must always be a
	// power of two).
	bundleShift := int(*((*uint8)(unsafe.Pointer(uintptr(start)))))
	bundleSize := 1 << bundleShift
	nBundles := (count + uint32(bundleSize) - 1) / uint32(bundleSize)

	pb := PrefixBytes{
		rows:        int(count),
		bundleShift: bundleShift,
		bundleMask:  ^((1 << bundleShift) - 1),
		rawBytes:    makeRawBytes(count+nBundles, unsafe.Pointer(uintptr(start)+1)),
	}
	pb.sharedPrefixLen = pb.rawBytes.offset(0)
	return pb
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

// RowBundlePrefix takes a row index and returns a []byte of the prefix shared
// among all the keys in the row's bundle. The returned slice should not be
// mutated.
func (b PrefixBytes) RowBundlePrefix(row int) []byte {
	// AND-ing the row with the bundle mask removes the least significant bits
	// of the row, which encode the row's index within the bundle.
	i := (row >> b.bundleShift) + (row & b.bundleMask)
	return b.rawBytes.slice(b.rawBytes.offset(i), b.rawBytes.offset(i+1))
}

// BundlePrefix returns the prefix of the i-th bundle in the column. The
// provided i must be in the range [0, Bundles()). The returned slice should not
// be mutated.
func (b PrefixBytes) BundlePrefix(i int) []byte {
	j := (i << b.bundleShift) + i
	return b.rawBytes.slice(b.rawBytes.offset(j), b.rawBytes.offset(j+1))
}

// BundleRowSuffixes returns the start (inclusive) and end (exclusive) indices
// of the row suffixes for all rows within the i-th bundle. The provided i must
// be in the range [0, Bundles()).
func (b PrefixBytes) BundleRowSuffixes(i int) (int, int) {
	start := i<<b.bundleShift + i + 2
	end := min(start+(1<<b.bundleShift), b.rawBytes.slices+1)
	return start, end
}

// At returns the i-th slice in PrefixBytes.
func (b PrefixBytes) At(i int) []byte {
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

// Bundles returns the count of bundles within the PrefixBytes.
func (b PrefixBytes) Bundles() int {
	return 1 + (b.rows-1)>>b.bundleShift
}

// Search searchs for the first key in the PrefixBytes that is greater than or
// equal to k, returning the index of the key and whether an equal key was
// found.
func (b PrefixBytes) Search(k []byte) (int, bool) {
	// First compare to the block-level shared prefix.
	n := min(len(k), b.sharedPrefixLen)
	c := bytes.Compare(k[:n], unsafe.Slice((*byte)(b.rawBytes.data), b.sharedPrefixLen))
	switch {
	case c < 0 || (c == 0 && n < b.sharedPrefixLen):
		// Search key is less than any prefix in the block.
		return 0, false
	case c > 0:
		// Search key is greater than any key in the block.
		return b.rows, false
	}
	// Trim the block-level shared prefix from the search key.
	k = k[b.sharedPrefixLen:]

	// Binary search among the first keys of each bundle.
	//
	// Define f(-1) == false and f(upper) == true.
	// Invariant: f(bi-1) == false, f(upper) == true.
	nBundles := b.Bundles()
	bi, upper := 0, nBundles
	upperEqual := false
	for bi < upper {
		h := int(uint(bi+upper) >> 1) // avoid overflow when computing h
		// bi ≤ h < upper

		// Retrieve the first key in the h-th (zero-indexed) bundle. We take
		// advantage of the fact that the first row is stored contiguously in
		// the data array (modulo the block prefix) to slice the entirety of the
		// first key:
		//
		//       b u n d l e p r e f i x f i r s t k e y r e m a i n d e r
		//       ^                       ^                                 ^
		//     offset(j)             offset(j+1)                       offset(j+2)
		//
		j := (h << b.bundleShift) + h
		bundleFirstKey := b.rawBytes.slice(b.rawBytes.offset(j), b.rawBytes.offset(j+2))
		c = bytes.Compare(k, bundleFirstKey)
		switch {
		case c > 0:
			bi = h + 1 // preserves f(bi-1) == false
		case c < 0:
			upper = h // preserves f(upper) == true
			upperEqual = false
		default:
			// c == 0
			upper = h // preserves f(upper) == true
			upperEqual = true
		}
	}
	if bi == 0 {
		// The very first key is ≥ k. Return it.
		return 0, upperEqual
	}
	// The first key of the bundle bi is ≥ k, but any of the keys in the
	// previous bundle besides the first could also be ≥ k. We can binary search
	// among them, but if the seek key doesn't share the previous bundle's
	// prefix there's no need.
	j := ((bi - 1) << b.bundleShift) + (bi - 1)
	bundlePrefix := b.rawBytes.slice(b.rawBytes.offset(j), b.rawBytes.offset(j+1))
	if len(bundlePrefix) > len(k) || !bytes.Equal(k[:len(bundlePrefix)], bundlePrefix) {
		// The search key doesn't share the previous bundle's prefix, so all of
		// the keys in the previous bundle must be less than k. We know the
		// first key of bi is ≥ k, so return it.
		if bi<<b.bundleShift < b.rows {
			return bi << b.bundleShift, upperEqual
		} else {
			return b.rows, false
		}
	}
	// Binary search among bundle bi-1's key remainders after stripping bundle
	// bi-1's prefix.
	//
	// Define f(l-1) == false and f(u) == true.
	// Invariant: f(l-1) == false, f(u) == true.
	k = k[len(bundlePrefix):]
	l := 1
	u := min(1<<b.bundleShift, b.rows-(bi-1)<<b.bundleShift)
	for l < u {
		h := int(uint(l+u) >> 1) // avoid overflow when computing h
		// l ≤ h < u

		// j is currently the index of the offset of bundle bi-i's prefix.
		//
		//     b u n d l e p r e f i x f i r s t k e y s e c o n d k e y
		//     ^                       ^               ^
		//  offset(j)              offset(j+1)     offset(j+2)
		//
		// The beginning of the zero-indexed i-th key of the bundle is at
		// offset(j+i+1).
		//
		hStart := b.rawBytes.offset(j + h + 1)
		hEnd := b.rawBytes.offset(j + h + 2)
		// There's a complication with duplicate keys. When keys are repeated,
		// the PrefixBytes encoding avoids re-encoding the duplciate key,
		// instead encoding an empty slice. While binary searching, if we land
		// on an empty slice, we need to back up until we find a non-empty slice
		// which is the key at index h. We iterate with p. If we eventually find
		// the duplicated key at index p < h and determine f(p) == true, then we
		// can set u=p (rather than h). If we determine f(p)==false, then we
		// know f(h)==false too and set l=h+1.
		p := h
		if hStart == hEnd {
			// Back up looking for an empty slice.
			for hStart == hEnd && p >= l {
				p--
				hEnd = hStart
				hStart = b.rawBytes.offset(j + p + 1)
			}
			// If we backed up to l-1, then all the rows in indexes [l, h] have
			// the same keys as index l-1. We know f(l-1) == false [see the
			// invariants above], so we can move l to h+1 and continue the loop
			// without performing any key comparisons.
			if p < l {
				l = h + 1
				continue
			}
		}
		rem := b.rawBytes.slice(hStart, hEnd)
		c = bytes.Compare(k, rem)
		switch {
		case c > 0:
			l = h + 1 // preserves f(l-1) == false
		case c < 0:
			u = p // preserves f(u) == true
			upperEqual = false
		default:
			// c == 0
			u = p // preserves f(u) == true
			upperEqual = true
		}
	}
	i := (bi-1)<<b.bundleShift + l
	if i < b.rows {
		return i, upperEqual
	} else {
		return b.rows, false
	}
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

func prefixBytesToBinFormatter(
	f *binfmt.Formatter, count uint32, sliceFormatter func([]byte) string,
) int {
	if sliceFormatter == nil {
		sliceFormatter = defaultSliceFormatter
	}
	pb := makePrefixBytes(count, unsafe.Pointer(f.Pointer(0)))
	dataSectionStartOffset := f.Offset() + int(uintptr(pb.rawBytes.data)-uintptr(pb.rawBytes.start))

	var n int
	f.CommentLine("PrefixBytes")
	n += f.HexBytesln(2, "16-bit offset count: %d", pb.rawBytes.nOffsets16)
	if off := uintptr(f.Pointer(0)); off < uintptr(pb.rawBytes.offsets) {
		n += f.HexBytesln(int(uintptr(pb.rawBytes.offsets)-off), "padding to align offsets table")
	}
	for i := 0; i < pb.rawBytes.nOffsets16; i++ {
		n += f.HexBytesln(2, "off[%d]: %d [overall %d]", i, pb.rawBytes.offset(i), dataSectionStartOffset+pb.rawBytes.offset(i))
	}
	for i := 0; i < int(count+1)-pb.rawBytes.nOffsets16; i++ {
		n += f.HexBytesln(4, "off[%d]: %d [overall %d]",
			i+pb.rawBytes.nOffsets16,
			pb.rawBytes.offset(i+pb.rawBytes.nOffsets16),
			dataSectionStartOffset+pb.rawBytes.offset(i))
	}
	bundlePrefixLen := pb.rawBytes.offset(0)
	n += f.HexBytesln(bundlePrefixLen, "data[00]: %s (block prefix)", sliceFormatter(pb.rawBytes.slice(0, bundlePrefixLen)))
	k := 1 + pb.rawBytes.Slices()
	s := bundlePrefixLen
	prevLen := s
	dots := strings.Repeat(".", bundlePrefixLen)
	for i := 1; i < k; i++ {
		e := pb.rawBytes.offset(i)
		if (i-1)%(1+(1<<pb.bundleShift)) == 0 {
			dots = strings.Repeat(".", bundlePrefixLen)
			n += f.HexBytesln(e-s, "data[%02d]: %s%s (bundle prefix)", i, dots, sliceFormatter(pb.rawBytes.At(i-1)))
			dots = strings.Repeat(".", e-s+bundlePrefixLen)
			prevLen = e - s + bundlePrefixLen
		} else if s == e {
			// An empty slice that's not a block or bundle prefix indicates a repeat key.
			n += f.HexBytesln(0, "data[%02d]: %s", i, strings.Repeat(".", prevLen))
		} else {
			n += f.HexBytesln(e-s, "data[%02d]: %s%s", i, dots, sliceFormatter(pb.rawBytes.At(i-1)))
			prevLen = len(dots) + e - s
		}
		s = e
	}
	return n
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

// BytesBuilder encodes a column of byte slices. If bundleSize is nonzero,
// BytesBuilder performs prefix compression, reducing the encoded block size.
//
// TODO(jackson): If prefix compression is very effective, the encoded size may
// remain very small while the physical size of the in-progress data slice may
// grow very large. This may pose memory usage problems during block building.
//
// TODO(jackson): Finish
type BytesBuilder struct {
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
	blockPrefixLenUpdated     int    // The row index of the last row that updated the block prefix.
	lastKeyLen                int    // The length of the last key added to the builder
	offsets                   []uint32
	nOffsets16                uint16
	maxShared                 uint16
	maxOffset16               uint32 // configurable for testing purposes
}

// Init initializes the BytesBuilder with the specified bundle size. If
// bundleSize is nonzero, the builder will produce a prefix-compressed column of
// data type DataTypePrefixBytes. When performing prefix-compression, the caller
// must use PutOrdered and pass keys in lexicographic order.
//
// When initialized with a zero bundle size, the builder will produce RawBytes
// columns without performing any prefix compression. Callers should use Put or
// PutConcat, and there are no requirements on the ordering of byte slices.
func (b *BytesBuilder) Init(bundleSize int) {
	if bundleSize > 0 && (bundleSize&(bundleSize-1)) != 0 {
		panicf("prefixbytes bundle size %d is not a power of 2", bundleSize)
	}
	*b = BytesBuilder{
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

// NumColumns implements ColumnWriter.
func (b *BytesBuilder) NumColumns() int { return 1 }

// Reset resets the builder to an empty state, preserving the existing bundle
// size.
func (b *BytesBuilder) Reset() {
	*b = BytesBuilder{
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
func (b *BytesBuilder) PutOrdered(key []byte, bytesSharedWithPrev int) []byte {
	if invariants.Enabled {
		if b.maxShared == 0 {
			panicf("maxShared must be positive")
		}
	}
	// This is PrefixBytes mode (eg, with prefix compression).

	switch {
	case b.nKeys == 0:
		// We're adding the first key to the block. Initialize the
		// block prefix to the length of this key.
		b.blockPrefixLen = uint32(min(len(key), int(b.maxShared)))
		b.blockPrefixLenUpdated = 0
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
	case b.nKeys%b.bundleSize == 0:
		// We're starting a new bundle so we can compute what the
		// encoded size of the previous bundle will be.
		bundlePrefixLen := b.offsets[b.currentBundlePrefixOffset] - b.offsets[b.currentBundlePrefixOffset-1]
		b.completedBundleLen += b.currentBundleLen - (b.currentBundleKeys-1)*int(bundlePrefixLen)

		// Update the block prefix length if necessary. The caller tells us how
		// many bytes of prefix this key shares with the previous key. The block
		// prefix can only shrink if the bytes shared with the previous key are
		// less than the block prefix length, in which case the new block prefix
		// is the number of bytes shared with the previous key.
		if uint32(bytesSharedWithPrev) < b.blockPrefixLen {
			b.blockPrefixLen = uint32(bytesSharedWithPrev)
			b.blockPrefixLenUpdated = b.nKeys
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
	default:
		// Adding a new key to an existing bundle.
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
				b.blockPrefixLenUpdated = b.nKeys
			}
		}
		b.nKeys++
		if bytesSharedWithPrev == len(key) {
			b.addOffset(b.offsets[len(b.offsets)-1])
			return b.data[len(b.data)-b.lastKeyLen:]
		}
		b.lastKeyLen = len(key)
		b.currentBundleLen += b.lastKeyLen
		b.currentBundleKeys++
		b.data = append(b.data, key...)
		b.addOffset(uint32(len(b.data)))
	}
	return b.data[len(b.data)-b.lastKeyLen:]
}

// PrevKey returns the previous key added to the builder through Put, PutConcat,
// or PutOrdered.
func (b *BytesBuilder) PrevKey() []byte {
	return b.data[len(b.data)-b.lastKeyLen:]
}

// Put appends the provided byte slice to the builder. The builder must be in
// RawBytes mode (initialized with a zero bundle size).
func (b *BytesBuilder) Put(s []byte) {
	// This is RawBytes mode (eg, no prefix compression).
	if invariants.Enabled && b.bundleSize > 0 {
		panic("Put called on BytesBuilder with non-zero bundle size")
	}
	if b.nKeys == 0 {
		// The first row. Initialize the block prefix to 0 in order to
		// streamline the logic in RawBytes.At() to avoid needing a special
		// case for row 0.
		b.addOffset(0)
	}
	b.nKeys++
	b.data = append(b.data, s...)
	b.lastKeyLen = len(s)
	b.addOffset(uint32(len(b.data)))
}

// PutConcat appends a single byte slice formed by the concatenation of the two
// byte slice arguments. The builder must be in RawBytes mode (initialized with
// a zero bundle size).
func (b *BytesBuilder) PutConcat(s1, s2 []byte) {
	// This is RawBytes mode (eg, no prefix compression).
	if invariants.Enabled && b.bundleSize > 0 {
		panic("Put called on BytesBuilder with non-zero bundle size")
	}
	if b.nKeys == 0 {
		// The first row. Initialize the block prefix to 0 in order to
		// streamline the logic in RawBytes.At() to avoid needing a special
		// case for row 0.
		b.addOffset(0)
	}
	b.nKeys++
	b.data = append(append(b.data, s1...), s2...)
	b.lastKeyLen = len(s1) + len(s2)
	b.addOffset(uint32(len(b.data)))
}

func (b *BytesBuilder) addOffset(offset uint32) {
	if offset <= b.maxOffset16 {
		b.nOffsets16++
	}
	b.offsets = append(b.offsets, offset)
}

func (b *BytesBuilder) setOffset(i int, offset uint32) {
	if offset <= b.maxOffset16 {
		b.nOffsets16 = max(b.nOffsets16, uint16(i))
	}
	b.offsets[i] = offset
}

// prefixCompressedSize TODO(peter) ...
func (b *BytesBuilder) prefixCompressedSize(dataLen uint32) uint32 {
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
func (b *BytesBuilder) prefixCompress(rows int, nOffsets int, buf []byte) uint32 {
	// TODO(jackson): Prefix-compress directly into Finish's destination buffer,
	// avoiding unnecessary copying.

	// Check if we've added enough offsets to perform prefix compression.
	if rows == 0 {
		return 0
	} else if rows == 1 {
		// If there's just 1 row, no prefix compression is necessary and we can
		// just encode that the entirety of the first key is the block prefix.
		b.offsets[0] = b.offsets[2]
		b.offsets[1] = b.offsets[2]
		return uint32(copy(buf, b.data))
	}
	blockPrefixLen := b.blockPrefixLen
	// If we're not writing out all the rows that have been Put (ie, rows <
	// b.nKeys), and one of the later rows that we're not writing updated the
	// block prefix length, then the cached block prefix length may be shorter
	// than necessary. In this case, we recompute it from the bundle prefixes.
	if b.blockPrefixLenUpdated >= rows {
		// Initialize the block prefix length to the first bundle prefix.
		blockPrefixLen = b.offsets[1]
		if rows > b.bundleSize {
			bi := (rows - 1) >> b.bundleShift
			i := bi<<b.bundleShift + bi
			blockPrefixLen = uint32(bytesSharedPrefix(
				b.data[b.offsets[0]:b.offsets[1]], b.data[b.offsets[i]:b.offsets[i+1]]))
		}
	}
	b.offsets[0] = blockPrefixLen
	destOffset := uint32(copy(buf, b.data[:blockPrefixLen]))
	var lastRowOffset uint32
	var shared uint32

	// All updates to b.offsets are performed /without/ using setOffset in order
	// to keep the encoded size inline with Size(). Some of the recomputed
	// offsets may be small enough that they could now be encoded in 16-bits.

	// Loop over the slices starting at the bundle prefix of the first bundle.
	// If the slice is a bundle prefix, carve off the suffix that excludes the
	// block prefix. Otherwise, carve off the suffix that excludes the block
	// prefix + bundle prefix.
	for i := 1; i < nOffsets; i++ {
		var suffix []byte
		if (i-1)%(b.bundleSize+1) == 0 {
			// This is a bundle prefix.
			suffix = b.data[lastRowOffset+blockPrefixLen : b.offsets[i]]
			shared = blockPrefixLen + uint32(len(suffix))
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

		destOffset += uint32(copy(buf[destOffset:], suffix))
		b.offsets[i] = destOffset
	}
	return destOffset
}

// Finish writes the serialized byte slices to buf starting at offset. The buf
// slice must be sufficiently large to store the serialized output. The caller
// should use [Size] to size buf appropriately before calling Finish.
//
// TODO(peter) ...
func (b *BytesBuilder) Finish(col int, rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	// TODO(jackson): Ensure we serialize [rows] rows, even if the caller has
	// put more slices than that.

	desc := ColumnDesc(DataTypeBytes)
	var nOffsets int
	var nOffsets16 uint16
	if b.bundleSize == 0 {
		if rows > 0 {
			nOffsets = rows + 1
		}
	} else {
		if rows > 0 {
			nOffsets = 2 + (rows-1)>>b.bundleShift + rows
		}
		desc = ColumnDesc(DataTypePrefixBytes)
		// Encode the bundle shift.
		buf[offset] = byte(b.bundleShift)
		offset++
	}
	nOffsets16 = uint16(min(nOffsets, int(b.nOffsets16)))
	nOffsets32 := nOffsets - int(nOffsets16)
	sizeOf16bitOffsetTable := uint32(nOffsets16) << align16Shift

	// Encode the count of 16-bit offsets.
	binary.LittleEndian.PutUint16(buf[offset:], nOffsets16)

	// Compute the positions of the offset tables (16-bit and 32-bit), and the
	// data section.
	offsetPadding := offset + align16
	offset16bitOffsets := align(offsetPadding, align16)
	offset32bitOffsets := offset16bitOffsets + sizeOf16bitOffsetTable
	offsetData := offset32bitOffsets
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
		offset32bitOffsets = align(offset32bitOffsets, align32)
		offset16bitOffsets = offset32bitOffsets - sizeOf16bitOffsetTable
		offsetData = offset32bitOffsets + uint32(nOffsets32<<align32Shift)
	}

	// Copy the data section. If bundleSize > 0, perform prefix compression as
	// we write out the data. This needs to happen before we write out the
	// offset tables because computing the prefix compression will mutate the
	// offsets.
	var endOffset uint32
	if b.bundleSize > 0 {
		// Prefix compress the variable width data and update the offsets
		// accordingly.
		endOffset = offsetData + b.prefixCompress(rows, nOffsets, buf[offsetData:])
	} else {
		endOffset = offsetData + uint32(copy(buf[offsetData:], b.data))
	}

	// Zero the padding between the count of 16-bit offsets and the start of the
	// offsets table for determinism when recycling buffers.
	for i := offsetPadding; i < offset16bitOffsets; i++ {
		buf[i] = 0
	}

	dest16 := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset16bitOffsets]))
	for i := 0; i < int(nOffsets16); i++ {
		if b.offsets[i] > b.maxOffset16 {
			panicf("%d: encoding offset %d as 16-bit, but it exceeds maxOffset16 %d", i, b.offsets[i], b.maxOffset16)
		}
		dest16.set(i, uint16(b.offsets[i]))
	}
	if nOffsets32 > 0 {
		if offset32bitOffsets != align(offset32bitOffsets, align32) {
			panicf("offset not aligned to 32: %d", offset32bitOffsets)
		}
		dest32 := makeUnsafeRawSlice[uint32](unsafe.Pointer(&buf[offset32bitOffsets]))
		copy(dest32.Slice(nOffsets32), b.offsets[b.nOffsets16:])
	}
	return endOffset, desc
}

// Size computes the size required to encode the byte slices beginning at the
// provided offset. The offset is required to ensure proper alignment. The
// returned int32 is the offset of the first byte after the end of the encoded
// data. To compute the size in bytes, subtract the [offset] passed into Size
// from the returned offset.
func (b *BytesBuilder) Size(rows int, offset uint32) uint32 {
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

func (b *BytesBuilder) WriteDebug(w io.Writer, rows int) {
	if b.bundleSize > 0 {
		fmt.Fprintf(w, "prefixbytes(%d): %d keys", b.bundleSize, b.nKeys)
	} else {
		fmt.Fprintf(w, "bytes: %d slices", b.nKeys)
	}
}
