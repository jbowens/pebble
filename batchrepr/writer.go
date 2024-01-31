// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package batchrepr

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/constants"
	"github.com/cockroachdb/pebble/internal/rawalloc"
)

const (
	// MaxBatchSize is the maximum size of a batch. It's limited by the uint32
	// offsets (see internal/batchskl.node, DeferredBatchOp, and
	// flushableBatchEntry).
	//
	// We limit the size to MaxUint32 (just short of 4GB) so that the exclusive
	// end of an allocation fits in uint32.
	//
	// On 32-bit systems, slices are naturally limited to MaxInt (just short of
	// 2GB).
	MaxBatchSize = constants.MaxUint32OrInt

	batchInitialSize = 1 << 10 // 1 KB
)

// SetSeqNum mutates the provided batch representation, storing the provided
// sequence number in its header. The provided byte slice must already be at
// least HeaderLen bytes long or else SetSeqNum will panic.
func SetSeqNum(repr []byte, seqNum uint64) {
	binary.LittleEndian.PutUint64(repr[:countOffset], seqNum)
}

// SetCount mutates the provided batch representation, storing the provided
// count in its header. The provided byte slice must already be at least
// HeaderLen bytes long or else SetCount will panic.
func SetCount(repr []byte, count uint32) {
	binary.LittleEndian.PutUint32(repr[countOffset:HeaderLen], count)
}

// Writer facilities the writing of KV records in the batch representation.
type Writer []byte

// Repr returns the batch representation. Before returning, Repr writes the
// provided entry count into the batch header.
func (w *Writer) Repr(count uint32) []byte {
	if len(*w) == 0 {
		w.init(HeaderLen)
	}
	SetCount(*w, count)
	return *w
}

// init initializes the batch to an empty batch. The capacity of the slice
// backing the initialized Writer will be at least as large as `size`.
func (w *Writer) init(size int) {
	n := batchInitialSize
	for n < size {
		n *= 2
	}
	if cap(*w) < n {
		*w = rawalloc.New(0, n)
	}
	*w = (*w)[:HeaderLen]
	clear(*w) // Zero the header
}

func (w *Writer) grow(n int) []byte {
	s := *w
	l, c := len(s), cap(s)
	newSize := l + n
	if uint64(newSize) >= MaxBatchSize {
		panic(ErrBatchTooLarge)
	}
	if newSize > c {
		newCap := 2 * c
		for newCap < newSize {
			newCap *= 2
		}
		s = rawalloc.New(l, min(newCap, MaxBatchSize))
		copy(s, *w)
	}
	return s[:newSize]
}

// Append takes a batch representation and appends its contents to w. The
// provided batch representation must be at least HeaderLen bytes long. Append
// returns the offset at which the appended batch begins.
func (w *Writer) Append(repr []byte) int {
	if len(*w) == 0 {
		w.init(len(repr) + HeaderLen)
	}
	offset := len(*w)
	*w = append(*w, repr[HeaderLen:]...)
	return offset
}

// PrepareKeyValueRecord grows the batch, allocating space and writing metadata
// for a key and value record with key and value of the provided length. The
// caller is expected to copy the key and value into their respective slices
// returned by this function before preparing any additional records. The
// returned offset is the index of the beginning of the record within the batch.
func (w *Writer) PrepareKeyValueRecord(
	keyLen, valueLen int, kind base.InternalKeyKind,
) (offset uint32, key, value []byte) {
	if len(*w) == 0 {
		w.init(keyLen + valueLen + 2*binary.MaxVarintLen64 + HeaderLen)
	}

	pos := len(*w)
	offset = uint32(pos)
	s := w.grow(1 + 2*binary.MaxVarintLen32 + keyLen + valueLen)

	s[pos] = byte(kind)
	pos++
	{
		// TODO(peter): Manually inlined version binary.PutUvarint(). This is
		// 20% faster on BenchmarkBatchSet on go1.13. Remove if future versions
		// show this to not be a performance win.
		x := uint32(keyLen)
		for x >= 0x80 {
			s[pos] = byte(x) | 0x80
			x >>= 7
			pos++
		}
		s[pos] = byte(x)
		pos++
	}

	key = s[pos : pos+keyLen]
	pos += keyLen

	{
		// TODO(peter): Manually inlined version binary.PutUvarint(). This is
		// 20% faster on BenchmarkBatchSet on go1.13. Remove if future versions
		// show this to not be a performance win.
		x := uint32(valueLen)
		for x >= 0x80 {
			s[pos] = byte(x) | 0x80
			x >>= 7
			pos++
		}
		s[pos] = byte(x)
		pos++
	}

	value = s[pos : pos+valueLen]
	// Shrink data since varints may be shorter than the upper bound, and update
	// *w to hold the new slice header.
	*w = s[:pos+valueLen]
	return offset, key, value
}

// PrepareKeyRecord grows the batch, allocating space and writing metadata for a
// new key-only record with a key of the provided length. The caller is expected
// to copy the key into the returned key slice before preparing any additional
// records. The returned offset is the index of the beginning of the record
// within the batch.
func (w *Writer) PrepareKeyRecord(
	keyLen int, kind base.InternalKeyKind,
) (offset uint32, key []byte) {
	if len(*w) == 0 {
		w.init(keyLen + binary.MaxVarintLen64 + HeaderLen)
	}
	pos := len(*w)
	offset = uint32(pos)
	s := w.grow(1 + binary.MaxVarintLen32 + keyLen)

	s[pos] = byte(kind)
	pos++

	{
		// TODO(peter): Manually inlined version binary.PutUvarint(). Remove if
		// future versions show this to not be a performance win. See
		// BenchmarkBatchSet.
		x := uint32(keyLen)
		for x >= 0x80 {
			s[pos] = byte(x) | 0x80
			x >>= 7
			pos++
		}
		s[pos] = byte(x)
		pos++
	}
	key = s[pos : pos+keyLen]
	// Shrink data since varint may be shorter than the upper bound, and update
	// *w to hold the new slice header.
	*w = s[:pos+keyLen]
	return offset, key
}
