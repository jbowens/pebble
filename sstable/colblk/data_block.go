// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

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
	ColumnWriter
	// ComparePrev compares the provided user to the previously-written user
	// key. It's equivalent to Compare(key, prevKey) where prevKey is the last
	// key passed to WriteKey.
	ComparePrev(key []byte) KeyComparison
	// SeparatePrev returns a key that is greater than or equal to the
	// previously written key and less than or equal to the provided key. The
	// sstable writer uses this method to compute index block separators.
	SeparatePrev(buf, key []byte, kcmp KeyComparison) []byte
	// WriteKey writes a user key into the KeyWriter's columns. The
	// keyPrefixLenSharedWithPrev parameter takes the number of bytes prefixing
	// the key's logical prefix (as defined by (base.Comparer).Split) that the
	// previously-written key's prefix shares.
	//
	// WriteKey is guaranteed to be called sequentially with increasing row
	// indexes, beginning at zero.
	//
	// WriteKey returns a copy of the key's prefix. The returned slice must be
	// be stable until the next call to WriteKey or the KeyWriter columns are
	// finished.
	WriteKey(row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int) (unsafePrefix []byte)
	// Release releases any resources held by the KeyWriter. It is called when
	// Pebble will no longer use the KeyWriter. Implementations may use Release
	// to pool KeyWriters.
	Release()
}

type KeyComparison struct {
	PrefixLen         int
	PrefixLenShared   int
	UserKeyComparison int
}

func (kcmp KeyComparison) PrefixEqual() bool { return kcmp.PrefixLen == kcmp.PrefixLenShared }

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
	//
	// TODO(jackson): With this interface, every new iterator must parse the key
	// column headers. Can an alternative interface allow the cache.Value to
	// hold a user-defined "key reader" type that has already constructed the
	// appropriate reader data structures which can be shared among all
	// KeyIterators?
	Init(b *DataBlockReader) error
	// SeekGE seeks the iterator to the first row with a key greater than or
	// equal to [key]. If [forward] is true, the caller guarantees the iterator
	// is positioned at a key known to be less than or equal to [key].
	// Implementations may use this knowledge to constrain the search.
	//
	// SeekGE returns the key of the row to which the iterator is positioned and
	// the index of the row.
	SeekGE(key []byte, forward bool) (resultKey []byte, row int)
	// SeekLT ... TODO(jackson)
	SeekLT(key []byte) (resultKey []byte, row int)
	// SetRow positions the iterator at the specified row, returning the key
	// at the row.
	SetRow(row int) []byte
	// Release releases the KeyIterator. It's called when the iterator is no
	// longer in use. Implementations may pool KeyIterator implementations.
	Release()
}

const (
	defaultKeySchemaColumnPrefix int = iota
	defaultKeySchemaColumnSuffix
)

// DefaultKeySchema returns the default key schema that decomposes a user key
// into its lexicographically sorted prefix, and a suffix.
func DefaultKeySchema(comparer *base.Comparer, prefixBundleSize int) KeySchema {
	return KeySchema{
		Columns: []ColumnConfig{
			defaultKeySchemaColumnPrefix: {DataType: DataTypePrefixBytes, BundleSize: 16},
			defaultKeySchemaColumnSuffix: {DataType: DataTypeBytes},
		},
		NewKeyWriter: func() KeyWriter {
			kw := &defaultKeyWriter{
				comparer: comparer,
				prefixes: BytesBuilder{},
				suffixes: BytesBuilder{},
			}
			kw.prefixes.Init(prefixBundleSize)
			kw.suffixes.Init(0)
			return kw
		},
		NewKeyIterator: func() KeyIterator {
			ki := &defaultKeyIterator{
				cmp:   comparer.Compare,
				split: comparer.Split,
			}
			return ki
		},
	}
}

// Assert that *defaultKeyWriter implements the KeyWriter interface.
var _ KeyWriter = (*defaultKeyWriter)(nil)

type defaultKeyWriter struct {
	comparer *base.Comparer
	prefixes BytesBuilder
	suffixes BytesBuilder
}

func (w *defaultKeyWriter) ComparePrev(key []byte) KeyComparison {
	pp := w.prefixes.PrevKey()
	var cmpv KeyComparison
	cmpv.PrefixLen = w.comparer.Split(key)
	cmpv.PrefixLenShared = bytesSharedPrefix(pp, key[:cmpv.PrefixLen])
	if cmpv.PrefixLenShared == cmpv.PrefixLen {
		// The keys share the same MVCC prefix. Compare the suffixes.
		cmpv.UserKeyComparison = w.comparer.Compare(key[cmpv.PrefixLen:], w.suffixes.PrevKey())
		return cmpv
	}
	// The keys have different MVCC prefixes. We haven't determined which is
	// greater, but we know the index at which they diverge. The base.Comparer
	// contract dictates that prefixes must be lexicograrphically ordered.
	if len(pp) == cmpv.PrefixLenShared {
		// cmpv.PrefixLen > cmpv.PrefixLenShared; key is greater.
		cmpv.UserKeyComparison = +1
	} else if cmpv.PrefixLen == cmpv.PrefixLenShared {
		// len(pp) > cmpv.PrefixLenShared; key is less.
		cmpv.UserKeyComparison = -1
	} else {
		// Both keys have at least 1 additional byte at which they diverge.
		// Compare the diverging byte.
		cmpv.UserKeyComparison = cmp.Compare(key[cmpv.PrefixLenShared], pp[cmpv.PrefixLenShared])
	}
	return cmpv
}

func (w *defaultKeyWriter) SeparatePrev(buf, key []byte, kcmp KeyComparison) []byte {
	pp := w.prefixes.PrevKey()
	switch {
	case key == nil:
		// If key is nil, this is the last block. Use successor.
		buf = w.comparer.Successor(buf, pp)
		if len(buf) <= len(pp) {
			// Shorter than the previous prefix; use it.
			return buf
		}
		return pp
	case kcmp.PrefixLen != kcmp.PrefixLenShared:
		sep := w.comparer.Separator(buf, pp, key)
		if len(sep) <= len(pp) {
			// Shorter than the previous prefix; use it.
			return sep
		}
		return pp
	case kcmp.UserKeyComparison == 0:
		// The previous key and `key` are identical. There is no shorter
		// separator than just key itself.
		return key
	default:
		// The keys share the same MVCC prefix, but the suffixes differ.
		// TODO(jackson): Create a Separator.
		buf := append(append(buf[:0], pp...), w.suffixes.PrevKey()...)
		return w.comparer.Separator(buf, buf, key)
	}
}

func (w *defaultKeyWriter) WriteKey(
	row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int,
) (prefix []byte) {
	copiedPrefix := w.prefixes.PutOrdered(key[:keyPrefixLen], keyPrefixLenSharedWithPrev)
	w.suffixes.Put(key[keyPrefixLen:])
	return copiedPrefix
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
		return w.prefixes.Finish(0, rows, offset, buf)
	case defaultKeySchemaColumnSuffix:
		return w.suffixes.Finish(0, rows, offset, buf)
	default:
		panic(fmt.Sprintf("unknown default key column: %d", col))
	}
}

func (w *defaultKeyWriter) Release() {
	// TODO(jackson): pool
}

// Assert that *defaultKeyIterator implements KeyIterator.
var _ KeyIterator = (*defaultKeyIterator)(nil)

type defaultKeyIterator struct {
	cmp   base.Compare
	split base.Split

	r            *DataBlockReader
	prefixes     PrefixBytes
	suffixes     RawBytes
	nRows        int
	sharedPrefix []byte
	key          []byte
	row          int
}

func (ki *defaultKeyIterator) Init(r *DataBlockReader) error {
	ki.r = r
	ki.prefixes = r.r.Column(defaultKeySchemaColumnPrefix).PrefixBytes()
	ki.suffixes = r.r.Column(defaultKeySchemaColumnSuffix).RawBytes()
	ki.nRows = int(r.r.header.Rows)
	ki.sharedPrefix = ki.prefixes.SharedPrefix()
	ki.row = -1
	return nil
}

func (ki *defaultKeyIterator) SeekGE(key []byte, forward bool) (resultKey []byte, row int) {
	si := ki.split(key)
	row, eq := ki.prefixes.Search(key[:si])
	if eq {
		return ki.seekGEOnSuffix(row, key[si:])
	}
	return ki.SetRow(row), row
}

// seekGEOnSuffix is a helper function for SeekGE when a seek key's prefix
// exactly matches a row. seekGEOnSuffix finds the first row at index or later
// with the same prefix as index and a suffix greater than or equal to [suffix],
// or if no such row exists, the next row with a different prefix.
func (ki *defaultKeyIterator) seekGEOnSuffix(index int, suffix []byte) (resultKey []byte, row int) {
	// The search key's prefix exactly matches the prefix of the row at index.
	// If the row at index has a suffix >= [suffix], then return the row.
	if ki.cmp(ki.suffixes.At(index), suffix) >= 0 {
		return ki.SetRow(index), index
	}
	// Otherwise, the row at [index] sorts before the search key and we need to
	// search forward. Binary search between [index+1, prefixChanged.Successor(index+1)].
	//
	// Define f(l-1) == false and f(u) == true.
	// Invariant: f(l-1) == false, f(u) == true.
	l := index + 1
	u := ki.r.prefixChanged.Successor(index + 1)
	for l < u {
		h := int(uint(l+u) >> 1) // avoid overflow when computing h
		// l ≤ h < u
		c := ki.cmp(suffix, ki.suffixes.At(h))
		switch {
		case c > 0:
			l = h + 1 // preserves f(l-1) == false
		case c < 0:
			u = h // preserves f(u) == true
		default:
			// c == 0
			u = h // preserves f(u) == true
		}
	}
	if l == ki.nRows {
		// Exhausted.
		return nil, ki.nRows
	}
	return ki.SetRow(l), l
}

func (ki *defaultKeyIterator) SeekLT(key []byte) (resultKey []byte, row int) {
	panic("unimplemented")
}

func (ki *defaultKeyIterator) SetRow(row int) []byte {
	if ki.row == row {
		// Already positioned here.
		return ki.key
	}
	ki.row = row
	if ki.row < 0 || ki.row >= ki.nRows {
		ki.key = nil
		return nil
	}

	// Retrieve the various components of the user key.
	bundlePrefix := ki.prefixes.RowBundlePrefix(row)
	rowSuffix := ki.prefixes.RowSuffix(row)
	suffix := ki.suffixes.At(row)
	// Ensure i.key has sufficient capacity.
	n := len(ki.sharedPrefix) + len(bundlePrefix) + len(rowSuffix) + len(suffix)
	if cap(ki.key) < n {
		ki.key = make([]byte, len(ki.sharedPrefix), n)
		copy(ki.key, ki.sharedPrefix)
	}
	ki.key = append(append(append(ki.key[:len(ki.sharedPrefix)], bundlePrefix...), rowSuffix...), suffix...)
	return ki.key
}

func (ki *defaultKeyIterator) Release() {
	// TODO(jackson): Pool
}

// DataBlockWriter writes columnar data blocks, encoding keys using a
// user-defined schema.
type DataBlockWriter struct {
	Schema    KeySchema
	KeyWriter KeyWriter
	// trailers is the column writer for InternalKey uint64 trailers.
	trailers Uint64Builder
	// prefixSame is the column writer for the prefix-changed bitmap that
	// indicates when a new key prefix begins. During block building, the bitmap
	// represents when the prefix stays the same, which is expected to be a
	// rarer case. Before Finish-ing the column, we invert the bitmap.
	prefixSame BitmapBuilder
	// values is the column writer for values. Every value is prefixed with a
	// single-byte value prefix.
	values BytesBuilder

	buf  []byte
	rows int
}

const (
	dataBlockColumnTrailer = iota
	dataBlockColumnPrefixChanged
	dataBlockColumnValue
	dataBlockColumnMax
)

// Init initializes the data block writer.
func (w *DataBlockWriter) Init(schema KeySchema) {
	w.Schema = schema
	w.KeyWriter = schema.NewKeyWriter()
	w.trailers.Init(UintDefaultNone)
	w.prefixSame.Reset()
	w.values.Init(0)
	w.rows = 0
	w.buf = w.buf[:0]
}

// Reset resets the data block writer to its initial state, retaining buffers.
func (w *DataBlockWriter) Reset() {
	w.KeyWriter.Reset()
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
	w.KeyWriter.WriteDebug(&buf, w.rows)

	fmt.Fprintf(&buf, "%d: trailers:       ", len(w.Schema.Columns)+dataBlockColumnTrailer)
	w.trailers.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "%d: prefix changed: ", len(w.Schema.Columns)+dataBlockColumnPrefixChanged)
	w.prefixSame.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "%d: values:         ", len(w.Schema.Columns)+dataBlockColumnValue)
	w.values.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	return buf.String()
}

// Add TODO(peter) ...
func (w *DataBlockWriter) Add(
	ikey base.InternalKey, valuePrefix, value []byte, kcmp KeyComparison,
) {
	w.KeyWriter.WriteKey(w.rows, ikey.UserKey, kcmp.PrefixLen, kcmp.PrefixLenShared)
	if kcmp.PrefixEqual() {
		w.prefixSame = w.prefixSame.Set(w.rows, true)
	}
	w.trailers.Set(w.rows, uint64(ikey.Trailer))
	w.values.PutConcat(valuePrefix, value)
	w.rows++
}

// Rows returns the number of rows in the current pending data block.
func (w *DataBlockWriter) Rows() int {
	return w.rows
}

// Size returns the size of the current pending data block.
func (w *DataBlockWriter) Size() int {
	off := blockHeaderSize(len(w.Schema.Columns)+dataBlockColumnMax, 0)
	off = w.KeyWriter.Size(w.rows, off)
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

	cols := len(w.Schema.Columns) + dataBlockColumnMax
	WriteHeader(w.buf, Header{
		Columns: uint16(cols),
		Rows:    uint32(w.rows),
	})
	var desc ColumnDesc
	pageOffset := blockHeaderSize(cols, 0)

	// Write the user-defined key columns.
	for i := range w.Schema.Columns {
		columnTagOff := blockHeaderSize(i, 0)
		binary.LittleEndian.PutUint32(w.buf[1+columnTagOff:], pageOffset)

		pageOffset, desc = w.KeyWriter.Finish(i, w.rows, pageOffset, w.buf)
		w.buf[columnTagOff] = byte(desc)
	}

	// Write the trailers.
	hOff := blockHeaderSize(len(w.Schema.Columns)+dataBlockColumnTrailer, 0)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.trailers.Finish(0, w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the prefix-same bitmap.
	// Invert it before writing it out, because we want it to represent when the
	// prefix changes.
	w.prefixSame.Invert(w.rows)
	hOff = blockHeaderSize(len(w.Schema.Columns)+dataBlockColumnPrefixChanged, 0)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.prefixSame.Finish(0, w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the value column.
	hOff = blockHeaderSize(len(w.Schema.Columns)+dataBlockColumnValue, 0)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.values.Finish(0, w.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	w.buf[pageOffset] = 0x00
	pageOffset++

	return w.buf
}

// DataBlockReaderSize is the size of a DataBlockReader struct. If allocating
// memory for a data block, the caller may want to additionally allocate memory
// for the corresponding DataBlockReader.
const DataBlockReaderSize = unsafe.Sizeof(DataBlockReader{})

// A DataBlockReader holds state for reading a columnar data block. It may be
// shared among multiple DataBlockIters.
type DataBlockReader struct {
	r             BlockReader
	trailers      UnsafeUint64s
	prefixChanged Bitmap
	values        RawBytes
}

// BlockReader returns a pointer to the underlying BlockReader.
func (r *DataBlockReader) BlockReader() *BlockReader {
	return &r.r
}

// Init initializes the data block reader with the given serialized data block.
func (r *DataBlockReader) Init(schema KeySchema, data []byte) {
	r.r.init(data, 0)
	r.trailers = r.r.Column(len(schema.Columns) + dataBlockColumnTrailer).UnsafeUint64s()
	r.values = r.r.Column(len(schema.Columns) + dataBlockColumnValue).RawBytes()
	r.prefixChanged = r.r.Column(len(schema.Columns) + dataBlockColumnPrefixChanged).Bitmap()
}

// DataBlockIter iterates over a columnar data block.
type DataBlockIter struct {
	r       *DataBlockReader
	keyIter KeyIterator
	row     int
	maxRow  int
	kv      base.InternalKV
}

var _ base.InternalIterator = (*DataBlockIter)(nil)

// Init initializes the data block iterator, configuring it to read from the
// provided reader.
func (i *DataBlockIter) Init(r *DataBlockReader, keyIterator KeyIterator) error {
	*i = DataBlockIter{
		r:       r,
		keyIter: keyIterator,
		row:     -1,
		maxRow:  int(r.r.header.Rows - 1),
	}
	return i.keyIter.Init(r)
}

// SeekGE implements the base.InternalIterator interface.
func (i *DataBlockIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.decodeRow(i.keyIter.SeekGE(key, flags.TrySeekUsingNext()))
}

// SeekPrefixGE implements the base.InternalIterator interface.
func (i *DataBlockIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	// This should never be called as prefix iteration is handled by
	// sstable.Iterator.
	// TODO(jackson): We can implement this and avoid propagating keys without
	// the prefix up to the merging iterator. It will avoid unnecessary key
	// comparisons fixing up the merging iterator heap.
	panic("pebble: SeekPrefixGE unimplemented")
}

// SeekLT implements the base.InternalIterator interface.
func (i *DataBlockIter) SeekLT(key []byte, _ base.SeekLTFlags) *base.InternalKV {
	return i.decodeRow(i.keyIter.SeekLT(key))
}

// First implements the base.InternalIterator interface.
func (i *DataBlockIter) First() *base.InternalKV {
	return i.decodeRow(i.keyIter.SetRow(0), 0)
}

// Last implements the base.InternalIterator interface.
func (i *DataBlockIter) Last() *base.InternalKV {
	row := i.maxRow
	return i.decodeRow(i.keyIter.SetRow(row), row)
}

// Next advances to the next KV pair in the block.
func (i *DataBlockIter) Next() *base.InternalKV {
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
// words. If a 64-bit word in the bitmap is zero then all of the rows
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
func (i *DataBlockIter) NextPrefix(_ []byte) *base.InternalKV {
	nextRow := i.r.prefixChanged.Successor(i.row + 1)
	if nextRow > i.maxRow {
		i.row = i.maxRow + 1
		return nil
	}
	i.row = nextRow
	return i.decodeRow(i.keyIter.SetRow(i.row), i.row)
}

// Prev moves the iterator to the previous KV pair in the block.
func (i *DataBlockIter) Prev() *base.InternalKV {
	if i.row <= 0 {
		i.row = -1
		return nil
	}
	row := i.row - 1
	return i.decodeRow(i.keyIter.SetRow(row), row)
}

// Error implements the base.InternalIterator interface. A DataBlockIter is
// infallible and always returns a nil error.
func (i *DataBlockIter) Error() error {
	return nil // infallible
}

// Close implements the base.InternalIterator interface.
func (i *DataBlockIter) Close() error {
	// TODO(peter)
	// i.handle.Release()
	// i.handle = bufferHandle{}
	// i.value = base.LazyValue{}
	// i.valueHandling.vbr = nil
	panic("pebble: Close unimplemented")
}

// SetBounds implements the base.InternalIterator interface.
func (i *DataBlockIter) SetBounds(lower, upper []byte) {
	// This should never be called as bounds are handled by sstable.Iterator.
	panic("pebble: SetBounds unimplemented")
}

// SetContext implements the base.InternalIterator interface.
func (i *DataBlockIter) SetContext(_ context.Context) {}

var dataBlockTypeString string = fmt.Sprintf("%T", (*DataBlockIter)(nil))

// String implements the base.InternalIterator interface.
func (i *DataBlockIter) String() string {
	return dataBlockTypeString
}

func (i *DataBlockIter) decodeRow(key []byte, row int) *base.InternalKV {
	i.row = row
	if key == nil {
		i.kv = base.InternalKV{}
		return nil
	}
	i.kv = base.MakeInternalKV(
		base.InternalKey{
			UserKey: key,
			Trailer: base.InternalKeyTrailer(i.r.trailers.At(row)),
		},
		// TODO(peter): Why does manually inlining Bytes.At help?
		i.r.values.slice(i.r.values.offset(i.row), i.r.values.offset(i.row+1)),
	)
	return &i.kv
}
