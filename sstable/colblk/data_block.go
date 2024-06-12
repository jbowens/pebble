// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/internal/base"
)

// KeySchema defines the schema of a user key, as defined by the user's
// application.
//
// TODO(jackson): Consider making this KVSchema and adding a WriteValue func. It
// feels like there's an opportunity to generalize the ShortAttribute so that
// when a value is stored out-of-band, the DataBlockWriter calls WriteValue to
// store the short attributes inlined within the data block. Otherwise, for
// inlined-values, the user-defined value columns would be implicitly null.
type KeySchema struct {
	Columns        []ColumnConfig
	NewKeyWriter   func() KeyWriter
	NewKeyIterator func() KeyIterator
}

// A KeyWriter maintains ColumnWriters for a data block for writing user keys
// into the database-specific key schema. Users may define their own key schema
// and implement KeyWriter to encode keys into custom columns that are aware of
// the structure of user keys.
type KeyWriter interface {
	MultiColumnWriter
	// WriteKey writes a user key into the KeyWriter's columns. It must return
	// true if the key's prefix (as defined by (base.Comparer).Split) is the
	// same as the previously-written key.
	//
	// WriteKey is guaranteed to be called sequentially with increasing row
	// indexes, beginning at zero.
	WriteKey(row int, key []byte) (samePrefix bool)
	// Release releases any resources held by the KeyWriter. It is called when
	// Pebble will no longer use the KeyWriter. Implementations may use Release
	// to pool KeyWriters.
	Release()
}

// KeyIterator iterates over the keys in a columnar data block.
//
// Users of Pebble who define their own key schema and must implement
// KeyIterator to seek over their decomposed keys.
//
// For the SeekGE and SeekLT operations, implementations are expected to return
// the re-constituted key and the zero-based index of the row to which it
// belongs.
type KeyIterator interface {
	// Init ... TODO(jackson)
	Init(b BlockReader) error
	// SeekGE ... TODO(jackson)
	SeekGE(key []byte) (resultKey []byte, row int)
	// SeekLT ... TODO(jackson)
	SeekLT(key []byte) (resultKey []byte, row int)
	// SetRow positions the iterator at the specified row, returning the key
	// at the row.
	SetRow(row int) []byte

	fmt.Stringer
}

const (
	defaultKeySchemaColumnPrefix int = iota
	defaultKeySchemaColumnSuffix
)

// DefaultKeySchema returns the default key schema that decomposes a user key
// into its lexicographically sorted prefix, and a suffix.
func DefaultKeySchema(split base.Split) KeySchema {
	return KeySchema{
		Columns: []ColumnConfig{
			defaultKeySchemaColumnPrefix: {DataType: DataTypePrefixBytes, BundleSize: 16},
			defaultKeySchemaColumnSuffix: {DataType: DataTypeBytes},
		},
		NewKeyWriter: func() KeyWriter {
			kw := &defaultKeyWriter{
				split:    split,
				prefixes: bytesBuilder{},
				suffixes: bytesBuilder{},
			}
			kw.prefixes.Init(16)
			kw.suffixes.Init(0)
			return kw
		},
	}
}

type defaultKeyWriter struct {
	split    base.Split
	prefixes bytesBuilder
	suffixes bytesBuilder
}

func (w *defaultKeyWriter) WriteKey(row int, key []byte) (samePrefix bool) {
	s := w.split(key)
	samePrefix = w.prefixes.PutOrdered(key[:s])
	w.suffixes.Put(key[s:])
	return samePrefix
}

func (w *defaultKeyWriter) NumColumns() int {
	return 2
}

func (w *defaultKeyWriter) Reset() {
	w.prefixes.Reset()
	w.suffixes.Reset()
}

func (w *defaultKeyWriter) WriteDebug(dst io.Writer, rows int) {
	fmt.Fprint(dst, "0: prefixes:       ")
	w.prefixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "1: suffixes:       ")
	w.suffixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
}

func (w *defaultKeyWriter) Size(rows int, offset uint32) uint32 {
	offset = w.prefixes.Size(rows, offset)
	return w.suffixes.Size(rows, offset)
}

func (w *defaultKeyWriter) Finish(
	col int, rows int, offset uint32, buf []byte,
) (uint32, ColumnDesc) {
	switch col {
	case defaultKeySchemaColumnPrefix:
		return w.prefixes.Finish(rows, offset, buf)
	case defaultKeySchemaColumnSuffix:
		return w.suffixes.Finish(rows, offset, buf)
	default:
		panic(fmt.Sprintf("unknown default key column: %d", col))
	}
}

func (w *defaultKeyWriter) Release() {
	// TODO(jackson): pool
}

// DataBlockWriter writes columnar data blocks, encoding keys using a
// user-defined schema.
type DataBlockWriter struct {
	// user key
	schema    KeySchema
	keyWriter KeyWriter
	// trailers is the column writer for InternalKey uint64 trailers.
	trailers Uint64Builder
	// prefixSame is the column writer for the prefix-changed bitmap that
	// indicates when a new key prefix begins. During block building, the bitmap
	// represents when the prefix stays the same, which is expected to be a
	// rarer case. Before Finish-ing the column, we invert the bitmap.
	prefixSame bitmapBuilder
	// values is the column writer for values. Every value is prefixed with a
	// single-byte value prefix.
	values bytesBuilder

	buf            []byte
	rows           int
	valuePrefixBuf [1]byte
}

const (
	dataBlockColumnTrailer = iota
	dataBlockColumnPrefixChanged
	dataBlockColumnValue
	dataBlockColumnMax
)

// Init initializes the data block writer.
func (w *DataBlockWriter) Init(schema KeySchema) {
	w.schema = schema
	w.keyWriter = schema.NewKeyWriter()
	w.trailers.Init(UintDefaultNone)
	w.valuePrefixBuf[0] = 'x' // TODO(jackson): Deal with the value prefix.
	w.values.Init(0)
}

// Reset resets the data block writer to its initial state, retaining buffers.
func (w *DataBlockWriter) Reset() {
	w.keyWriter.Reset()
	w.trailers.Reset()
	w.prefixSame.Reset()
	w.values.Reset()
	w.rows = 0
	w.buf = w.buf[:0]
}

// String outputs a human-readable summary of internal DataBlockWriter state.
func (w *DataBlockWriter) String() string {
	var buf bytes.Buffer
	size := uint32(w.Size())
	fmt.Fprintf(&buf, "size=%d:\n", size)
	w.keyWriter.WriteDebug(&buf, w.rows)

	fmt.Fprintf(&buf, "%d: trailers:       ", len(w.schema.Columns)+dataBlockColumnTrailer)
	w.trailers.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "%d: prefix changed: ", len(w.schema.Columns)+dataBlockColumnPrefixChanged)
	w.prefixSame.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "%d: values:         ", len(w.schema.Columns)+dataBlockColumnValue)
	w.values.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	return buf.String()
}

// Add TODO(peter) ...
func (w *DataBlockWriter) Add(ikey base.InternalKey, value []byte) {
	if w.keyWriter.WriteKey(w.rows, ikey.UserKey) {
		w.prefixSame = w.prefixSame.Set(w.rows, true)
	}
	w.trailers.Set(w.rows, ikey.Trailer)
	w.values.PutConcat(w.valuePrefixBuf[:], value)
	w.rows++
}

// Size returns the size of the current pending data block.
func (w *DataBlockWriter) Size() int {
	off := blockHeaderSize(len(w.schema.Columns)+dataBlockColumnMax, 0)
	off = w.keyWriter.Size(w.rows, off)
	off = w.trailers.Size(w.rows, off)
	off = w.prefixSame.Size(w.rows, off)
	off = w.values.Size(w.rows, off)
	off += 1
	return int(off)
}

// Finish serializes the pending data block.
func (w *DataBlockWriter) Finish() []byte {
	size := w.Size()
	if cap(w.buf) < size {
		w.buf = make([]byte, size)
	}
	w.buf = w.buf[:size]

	cols := len(w.schema.Columns) + dataBlockColumnMax
	WriteHeader(w.buf, Header{
		Columns: uint16(cols),
		Rows:    uint32(w.rows),
	})
	var desc ColumnDesc
	pageOffset := blockHeaderSize(cols, 0)

	// Write the user-defined key columns.
	for i := range w.schema.Columns {
		columnTagOff := blockHeaderSize(i, 0)
		binary.LittleEndian.PutUint32(w.buf[1+columnTagOff:], pageOffset)

		pageOffset, desc = w.keyWriter.Finish(i, w.rows, pageOffset, w.buf)
		w.buf[columnTagOff] = byte(desc)
	}

	// Write the trailers.
	hOff := blockHeaderSize(len(w.schema.Columns)+dataBlockColumnTrailer, 0)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.trailers.Finish(w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the prefix-same bitmap.
	// Invert it before writing it out, because we want it to represent when the
	// prefix changes.
	w.prefixSame.Invert()
	hOff = blockHeaderSize(len(w.schema.Columns)+dataBlockColumnPrefixChanged, 0)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.prefixSame.Finish(w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the value column.
	hOff = blockHeaderSize(len(w.schema.Columns)+dataBlockColumnValue, 0)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.values.Finish(w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	w.buf[pageOffset] = 0x00
	pageOffset++

	return w.buf
}

type dataBlockIter struct {
	keyIter       KeyIterator
	trailers      UnsafeUint64s
	prefixChanged Bitmap
	values        RawBytes
	row           int
	maxRow        int
	kv            base.InternalKV
}

var _ base.InternalIterator = (*dataBlockIter)(nil)

// Init TODO(peter) ...
func (i *dataBlockIter) Init(nKeyColumns int, data []byte) error {
	var b BlockReader
	b.init(data, 0)
	trailers := b.Column(nKeyColumns + dataBlockColumnTrailer)
	i.trailers = trailers.UnsafeUint64s()
	i.values = b.Column(nKeyColumns + dataBlockColumnValue).RawBytes()
	i.prefixChanged = b.Column(nKeyColumns + dataBlockColumnPrefixChanged).Bitmap()
	i.row = -1
	i.maxRow = int(trailers.N - 1)
	i.kv = base.InternalKV{}
	return i.keyIter.Init(b)
}

func (i *dataBlockIter) SeekGE(key []byte, _ base.SeekGEFlags) *base.InternalKV {
	return i.decodeRow(i.keyIter.SeekGE(key))
}

func (i *dataBlockIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	// This should never be called as prefix iteration is handled by
	// sstable.Iterator.
	panic("pebble: SeekPrefixGE unimplemented")
}

func (i *dataBlockIter) SeekLT(key []byte, _ base.SeekLTFlags) *base.InternalKV {
	return i.decodeRow(i.keyIter.SeekLT(key))
}

func (i *dataBlockIter) First() *base.InternalKV {
	return i.decodeRow(i.keyIter.SetRow(0), 0)
}

func (i *dataBlockIter) Last() *base.InternalKV {
	row := i.maxRow
	return i.decodeRow(i.keyIter.SetRow(row), row)
}

func (i *dataBlockIter) Next() *base.InternalKV {
	if i.row >= i.maxRow {
		i.row = i.maxRow + 1
		return nil
	}
	row := i.row + 1
	return i.decodeRow(i.keyIter.SetRow(row), row)
}

// NextPrefix moves the iterator to the next row with a different prefix than
// the key at the current iterator position. The columnar block implementation
// uses a newPrefix bitmap to identify the next row with a differing prefix
// from the current row's key. If newPrefix[i] is set then row's i key prefix
// is different that row i-1. The bitmap is organized as a slice of 64-bit
// words. If a 64-bit word in the bitmap is zero then all of the row's
// corresponding to the bits in that word have the same prefix and we can skip
// ahead. If a row is non-zero a small bit of bit shifting and masking
// combined with bits.TrailingZeros64 can identify the the next bit that is
// set after the current row. The bitmap uses 1 bit/row (0.125 bytes/row). A
// 32KB block containing 1300 rows (25 bytes/row) would need a bitmap of 21
// 64-bit words. Even in the worst case where every word is 0 this bitmap can
// be scanned in ~20 ns (1 ns/word) leading to a total NextPrefix time of ~30
// ns if a row is found and SetRow/decodeRow are called. In more normal cases,
// NextPrefix takes ~15% longer that a single Next call.
//
// TODO(peter): Add a second-level summary bitmap on top of the per-row bitmap
// which stores a bit per word in the per-row bitmap indicating if any of the
// bits are set in that word. This will allow skipping over up to 4096 (64x64)
// rows at once. A 32KB block will always contain fewer than 4096 rows. We'll
// need to do at most one bits.TrailingZeros64 in the current word, one
// bits.TrailingZeros64 in the summary bitmap, and then one
// bits.TrailingZeros64 in the next non-empty word. So in essentially constant
// time we can find the row for the next prefix.
//
// For comparision, the blockIter.nextPrefixV3 optimizations work by setting a
// bit in the value prefix byte that indicates that the current key has the
// same prefix as the previous key. Additionally, a bit is stolen from the
// restart table entries indicating whether a restart table entry has the same
// key prefix as the previous entry. Checking the value prefix byte bit
// requires locating that byte which requires decoding 3 varints per key/value
// pair.
func (i *dataBlockIter) NextPrefix(_ []byte) *base.InternalKV {
	nextRow := i.prefixChanged.Successor(i.row + 1)
	if nextRow > i.maxRow {
		i.row = i.maxRow + 1
		return nil
	}
	i.row = nextRow
	return i.decodeRow(i.keyIter.SetRow(i.row), i.row)
}

func (i *dataBlockIter) Prev() *base.InternalKV {
	if i.row <= 0 {
		i.row = -1
		return nil
	}
	row := i.row - 1
	return i.decodeRow(i.keyIter.SetRow(row), row)
}

func (i *dataBlockIter) Error() error {
	return nil // infallible
}

func (i *dataBlockIter) Close() error {
	// TODO(peter)
	// i.handle.Release()
	// i.handle = bufferHandle{}
	// i.value = base.LazyValue{}
	// i.valueHandling.vbr = nil
	panic("pebble: Close unimplemented")
}

func (i *dataBlockIter) SetBounds(lower, upper []byte) {
	// This should never be called as bounds are handled by sstable.Iterator.
	panic("pebble: SetBounds unimplemented")
}

func (i *dataBlockIter) SetContext(_ context.Context) {
}

func (i *dataBlockIter) String() string {
	return i.keyIter.String()
}

func (i *dataBlockIter) decodeRow(key []byte, row int) *base.InternalKV {
	i.row = row
	if key == nil {
		i.kv = base.InternalKV{}
		return nil
	}
	i.kv = base.MakeInternalKV(
		base.InternalKey{
			UserKey: key,
			Trailer: uint64(i.trailers.At(row)),
		},
		// TODO(peter): Why does manually inlining Bytes.At help?
		i.values.slice(i.values.offset(i.row), i.values.offset(i.row+1)),
	)
	return &i.kv
}
