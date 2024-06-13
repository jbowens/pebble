// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

const (
	keyspanBlockVersionTODO = 0x01
	keyspanHeaderSize       = 4
	keyspanColUserKeys      = 0
	keyspanColStartIndices  = 1
	keyspanColTrailers      = 2
	keyspanColSuffixes      = 3
	keyspanColValues        = 4
	keyspanColumnCount      = 5
)

var keyspanSchema = [keyspanColumnCount]ColumnConfig{
	// User keys cannot be prefix-compressed because they are not guaranteed to
	// be lexicographically sorted even if they're sorted according to the
	// user-provided Comparer. These user keys may contain suffixes.
	//
	// The user keys column's row count is different than the other columns.
	keyspanColUserKeys: {DataType: DataTypeBytes},

	// Columns encoding one row per keyspan.Key.
	keyspanColStartIndices: {DataType: DataTypeUint32},
	keyspanColTrailers:     {DataType: DataTypeUint64},
	keyspanColSuffixes:     {DataType: DataTypeBytes},
	keyspanColValues:       {DataType: DataTypeBytes},
}

// A KeyspanBlockWriter writes keyspan blocks.
type KeyspanBlockWriter struct {
	equal base.Equal

	userKeys      BytesBuilder
	startIndexes  Uint32Builder
	trailers      Uint64Builder
	suffixes      DefaultNull[*BytesBuilder]
	suffixesBytes BytesBuilder
	values        DefaultNull[*BytesBuilder]
	valuesBytes   BytesBuilder

	buf               []byte
	keyCount          int
	unsafeLastUserKey []byte
}

// Init initializes a keyspan block writer.
func (w *KeyspanBlockWriter) Init(equal base.Equal) {
	w.equal = equal
	w.userKeys.Init(0)
	w.suffixesBytes.Init(0)
	w.suffixes = MakeDefaultNull(DataTypeBytes, &w.suffixesBytes)
	w.valuesBytes.Init(0)
	w.values = MakeDefaultNull(DataTypeBytes, &w.valuesBytes)
	w.Reset()
}

func (w *KeyspanBlockWriter) Reset() {
	w.userKeys.Reset()
	w.startIndexes.Reset()
	w.trailers.Reset()
	w.suffixes.Reset()
	w.values.Reset()
	w.buf = w.buf[:0]
	w.keyCount = 0
	w.unsafeLastUserKey = nil
}

// AddSpan appends a new Span to the pending block. Spans must already be
// fragmented (non-overlapping) and added in sorted order.
func (w *KeyspanBlockWriter) AddSpan(s *keyspan.Span) {
	// When keyspans are fragmented, abutting spans share a user key. One span's
	// end key is the next span's start key.  Check if the previous user key
	// equals this span's start key, and avoid encoding it again if so.
	if w.unsafeLastUserKey == nil || !w.equal(w.unsafeLastUserKey, s.Start) {
		w.userKeys.Put(s.Start)
	}
	startIdx := w.userKeys.nKeys - 1
	// The end key must be strictly greater than the start key and spans are
	// already sorted, so the end key is guaranteed to not be present in the
	// column yet. We need to encode it.
	w.userKeys.Put(s.End)

	// Hold on to a slice of the copy of s.End we just added to the bytes
	// builder so that we can compare it to the next span's start key.
	w.unsafeLastUserKey = w.userKeys.data[len(w.userKeys.data)-len(s.End):]

	// Encode each keyspan.Key in the span.
	for i := range s.Keys {
		w.startIndexes.Set(w.keyCount, uint32(startIdx))
		w.trailers.Set(w.keyCount, uint64(s.Keys[i].Trailer))
		if len(s.Keys[i].Suffix) > 0 {
			sw, _ := w.suffixes.NotNull(w.keyCount)
			sw.Put(s.Keys[i].Suffix)
		}
		if len(s.Keys[i].Value) > 0 {
			vw, _ := w.values.NotNull(w.keyCount)
			vw.Put(s.Keys[i].Value)
		}
		w.keyCount++
	}
}

// Size returns the size of the pending block.
func (w *KeyspanBlockWriter) Size() int {
	off := blockHeaderSize(keyspanColumnCount, keyspanHeaderSize)
	off = w.userKeys.Size(w.userKeys.nKeys, off)
	off = w.startIndexes.Size(w.keyCount, off)
	off = w.trailers.Size(w.keyCount, off)
	off = w.suffixes.Size(w.keyCount, off)
	off = w.values.Size(w.keyCount, off)
	off++
	return int(off)
}

// Finish finalizes the pending block and returns the encoded block.
func (w *KeyspanBlockWriter) Finish() []byte {
	size := w.Size()
	if cap(w.buf) < size {
		w.buf = make([]byte, size)
	}
	w.buf = w.buf[:size]

	// The keyspan block has a 4-byte custom header used to encode the number of
	// user keys encoded within the user key column. All other columns have the
	// number of rows indicated by the shared columnar block header, except for
	// the user key column.
	binary.LittleEndian.PutUint32(w.buf, uint32(w.userKeys.nKeys))

	WriteHeader(w.buf[keyspanHeaderSize:], Header{
		Columns: uint16(keyspanColumnCount),
		Rows:    uint32(w.keyCount),
	})
	var desc ColumnDesc
	pageOffset := blockHeaderSize(keyspanColumnCount, keyspanHeaderSize)

	// Write the user keys.
	hOff := blockHeaderSize(keyspanColUserKeys, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.userKeys.Finish(0, w.userKeys.nKeys, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the start indices.
	hOff = blockHeaderSize(keyspanColStartIndices, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.startIndexes.Finish(0, w.keyCount, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the trailers.
	hOff = blockHeaderSize(keyspanColTrailers, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.trailers.Finish(0, w.keyCount, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the suffixes.
	hOff = blockHeaderSize(keyspanColSuffixes, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.suffixes.Finish(0, w.keyCount, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	// Write the values.
	hOff = blockHeaderSize(keyspanColValues, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset, desc = w.values.Finish(0, w.keyCount, pageOffset, w.buf)
	w.buf[hOff] = byte(desc)

	w.buf[pageOffset] = keyspanBlockVersionTODO
	return w.buf
}

// String returns a string representation of the pending block's state.
func (w *KeyspanBlockWriter) String() string {
	var buf bytes.Buffer
	size := uint32(w.Size())
	fmt.Fprintf(&buf, "size=%d:\n", size)

	fmt.Fprint(&buf, "0: user keys:      ")
	w.userKeys.WriteDebug(&buf, w.userKeys.nKeys)
	fmt.Fprintln(&buf)
	fmt.Fprint(&buf, "1: start indices:  ")
	w.startIndexes.WriteDebug(&buf, w.keyCount)
	fmt.Fprintln(&buf)
	fmt.Fprint(&buf, "2: trailers:       ")
	w.trailers.WriteDebug(&buf, w.keyCount)
	fmt.Fprintln(&buf)
	fmt.Fprint(&buf, "3: suffixes:       ")
	w.suffixes.WriteDebug(&buf, w.keyCount)
	fmt.Fprintln(&buf)
	fmt.Fprint(&buf, "4: values:         ")
	w.values.WriteDebug(&buf, w.keyCount)
	fmt.Fprintln(&buf)

	return buf.String()
}

// A KeyspanReader exposes facilities for reading a keyspan block. A
// KeyspanReader is safe for concurrent use.
type KeyspanReader struct {
	blockReader      BlockReader
	nUserKeys        uint32
	userKeys         RawBytes
	startIndices     UnsafeUint32s
	trailers         UnsafeUint64s
	suffixes         Vec
	suffixesRawBytes RawBytes
	values           Vec
	valuesRawBytes   RawBytes
}

// Init initializes the keyspan reader with the given block data.
func (r *KeyspanReader) Init(data []byte) {
	r.nUserKeys = binary.LittleEndian.Uint32(data[:4])
	r.blockReader.init(data, keyspanHeaderSize)
	// The user key column has a different number of rows than the other
	// columns, so we need to take care to pass the appropriate one to
	// BlockReader.column.
	r.userKeys = r.blockReader.column(keyspanColUserKeys, int(r.nUserKeys)).RawBytes()
	r.startIndices = r.blockReader.Column(keyspanColStartIndices).UnsafeUint32s()
	r.trailers = r.blockReader.Column(keyspanColTrailers).UnsafeUint64s()
	r.suffixes = r.blockReader.Column(keyspanColSuffixes)
	if !r.suffixes.NullBitmap.Full() {
		r.suffixesRawBytes = r.suffixes.RawBytes()
	}
	r.values = r.blockReader.Column(keyspanColValues)
	if !r.values.NullBitmap.Full() {
		r.valuesRawBytes = r.values.RawBytes()
	}
}

// DebugString prints a human-readable explanation of the keyspan block's binary
// representation.
func (r *KeyspanReader) DebugString() string {
	f := binfmt.New(r.blockReader.data()).LineWidth(20)
	f.CommentLine("keyspan block header")
	f.HexBytesln(4, "user key count: %d", r.nUserKeys)
	r.blockReader.headerToBinFormatter(f)

	for i := 0; i < keyspanColumnCount; i++ {
		// Not all columns in a keyspan block have the same number of rows; the
		// user key column is different (and its length is held in the keyspan
		// block header that precedes the ordinary columnar block header).
		rows := int(r.blockReader.header.Rows)
		if i == keyspanColUserKeys {
			rows = int(r.nUserKeys)
		}
		r.blockReader.columnToBinFormatter(f, i, rows)
	}
	return f.String()
}

func (r *KeyspanReader) searchUserKeys(cmp base.Compare, key []byte) (index int, equal bool) {
	i, j := 0, int(r.nUserKeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		switch cmp(key, r.userKeys.At(h)) {
		case +1:
			i = h + 1
		case 0:
			return h, true
		case -1:
			j = h
		default:
			panic("unreachable")
		}
	}
	return i, cmp(key, r.userKeys.At(i)) == 0
}

// A KeyspanIter is an iterator over a keyspan block. It implements the
// keyspan.FragmentIterator interface.
type KeyspanIter struct {
	r      *KeyspanReader
	cmp    base.Compare
	span   keyspan.Span
	i      int
	keyBuf [2]keyspan.Key
}

// Assert that KeyspanIter implements the FragmentIterator interface.
var _ keyspan.FragmentIterator = (*KeyspanIter)(nil)

// Init initializes the iterator with the given comparison function and keyspan
// reader.
func (i *KeyspanIter) Init(cmp base.Compare, r *KeyspanReader) {
	i.r = r
	i.cmp = cmp
	i.i = -1
	if i.span.Keys == nil {
		i.span.Keys = i.keyBuf[:0]
	}
}

// SeekGE moves the iterator to the first span covering a key greater than
// or equal to the given key. This is equivalent to seeking to the first
// span with an end key greater than the given key.
func (i *KeyspanIter) SeekGE(key []byte) (*keyspan.Span, error) {
	j, eq := i.r.searchUserKeys(i.cmp, key)
	if eq {
		j++
	}
	// We might already know that [key] sorts after all of the blocks' spans.
	// If j == i.r.nUserKeys, then it sorts after all boundaries, including end
	// keys, and must be greater than all the blocks spans.
	if j == int(i.r.nUserKeys) {
		i.i = int(i.r.blockReader.header.Rows)
		return nil, nil
	}

	i.i = sort.Search(int(i.r.blockReader.header.Rows), func(r int) bool {
		return i.r.startIndices.At(r)+1 >= uint32(j)
	})
	if i.i >= int(i.r.blockReader.header.Rows) {
		return nil, nil
	}

	// The span at i.i is the first span with an end key > key.
	startIndex := i.r.startIndices.At(i.i)
	i.span = keyspan.Span{
		Start: i.r.userKeys.At(int(startIndex)),
		End:   i.r.userKeys.At(int(startIndex + 1)),
		Keys:  i.span.Keys[:0],
	}
	i.gatherKeysForward(int(startIndex))
	return &i.span, nil
}

// SeekLT moves the iterator to the last span covering a key less than the
// given key. This is equivalent to seeking to the last span with a start
// key less than the given key.
func (i *KeyspanIter) SeekLT(key []byte) (*keyspan.Span, error) {
	j, _ := i.r.searchUserKeys(i.cmp, key)
	i.i = sort.Search(int(i.r.blockReader.header.Rows), func(r int) bool {
		return i.r.startIndices.At(r) >= uint32(j)
	})
	i.i -= 1
	if i.i < 0 {
		return nil, nil
	}

	// The span at i.i is the first span with an end key > key.
	startIndex := i.r.startIndices.At(i.i)
	i.span = keyspan.Span{
		Start: i.r.userKeys.At(int(startIndex)),
		End:   i.r.userKeys.At(int(startIndex + 1)),
		Keys:  i.span.Keys[:0],
	}
	i.gatherKeysBackward(int(startIndex))
	return &i.span, nil
}

// First moves the iterator to the first span.
func (i *KeyspanIter) First() (*keyspan.Span, error) {
	i.i = 0
	if i.i >= int(i.r.blockReader.header.Rows) {
		return nil, nil
	}
	startIndex := i.r.startIndices.At(i.i)
	i.span = keyspan.Span{
		Start: i.r.userKeys.At(int(startIndex)),
		End:   i.r.userKeys.At(int(startIndex + 1)),
		Keys:  i.span.Keys[:0],
	}
	i.gatherKeysForward(int(startIndex))
	return &i.span, nil
}

// Last moves the iterator to the last span.
func (i *KeyspanIter) Last() (*keyspan.Span, error) {
	i.i = int(i.r.blockReader.header.Rows) - 1
	if i.i < 0 {
		return nil, nil
	}
	startIndex := i.r.startIndices.At(i.i)
	i.span = keyspan.Span{
		Start: i.r.userKeys.At(int(startIndex)),
		End:   i.r.userKeys.At(int(startIndex + 1)),
		Keys:  i.span.Keys[:0],
	}
	i.gatherKeysBackward(int(startIndex))
	return &i.span, nil
}

// Next moves the iterator to the next span.
func (i *KeyspanIter) Next() (*keyspan.Span, error) {
	i.i = min(i.i+1, int(i.r.blockReader.header.Rows))
	if i.i >= int(i.r.blockReader.header.Rows) {
		return nil, nil
	}
	startIndex := i.r.startIndices.At(i.i)
	i.span = keyspan.Span{
		Start: i.r.userKeys.At(int(startIndex)),
		End:   i.r.userKeys.At(int(startIndex + 1)),
		Keys:  i.span.Keys[:0],
	}
	i.gatherKeysForward(int(startIndex))
	return &i.span, nil
}

// Prev moves the iterator to the previous span.
func (i *KeyspanIter) Prev() (*keyspan.Span, error) {
	i.i = max(i.i-1, -1)
	if i.i < 0 {
		return nil, nil
	}
	startIndex := i.r.startIndices.At(i.i)
	i.span = keyspan.Span{
		Start: i.r.userKeys.At(int(startIndex)),
		End:   i.r.userKeys.At(int(startIndex + 1)),
		Keys:  i.span.Keys[:0],
	}
	i.gatherKeysBackward(int(startIndex))
	return &i.span, nil
}

// gatherKeysForward gathers all keys for the current span bounds in the forward
// direction.
//
// TODO(jackson): improve documentation
func (i *KeyspanIter) gatherKeysForward(startIndex int) {
	for {
		k := keyspan.Key{Trailer: base.InternalKeyTrailer(i.r.trailers.At(i.i))}
		if j := i.r.suffixes.Rank(i.i); j >= 0 {
			k.Suffix = i.r.suffixesRawBytes.At(j)
		}
		if j := i.r.values.Rank(i.i); j >= 0 {
			k.Value = i.r.valuesRawBytes.At(j)
		}
		i.span.Keys = append(i.span.Keys, k)
		if i.i+1 == int(i.r.blockReader.header.Rows) || i.r.startIndices.At(i.i+1) != uint32(startIndex) {
			return
		}
		i.i += 1
	}
}

// gatherKeysBackward gathers all keys for the current span bounds in the
// backward direction.
// /
// TODO(jackson): improve documentation
func (i *KeyspanIter) gatherKeysBackward(startIndex int) {
	for {
		k := keyspan.Key{Trailer: base.InternalKeyTrailer(i.r.trailers.At(i.i))}
		if j := i.r.suffixes.Rank(i.i); j >= 0 {
			k.Suffix = i.r.suffixesRawBytes.At(j)
		}
		if j := i.r.values.Rank(i.i); j >= 0 {
			k.Value = i.r.valuesRawBytes.At(j)
		}
		i.span.Keys = append(i.span.Keys, k)
		if i.i-1 < 0 || i.r.startIndices.At(i.i-1) != uint32(startIndex) {
			// We've collected all the keys for the current span, but they're in
			// reverse order because we traversed backwards.
			// Reverse them.
			for j := 0; j < len(i.span.Keys)/2; j++ {
				i.span.Keys[j], i.span.Keys[len(i.span.Keys)-j-1] = i.span.Keys[len(i.span.Keys)-j-1], i.span.Keys[j]
			}
			return
		}
		i.i -= 1
	}
}

// Close closes the iterator.
func (i *KeyspanIter) Close() {}

// WrapChildren implements keyspan.FragmentIterator.
func (i *KeyspanIter) WrapChildren(keyspan.WrapFn) {}
