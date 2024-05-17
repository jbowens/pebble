// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import "unsafe"

// Vec holds data for a single column. Vec provides accessors for the native
// data such as Int32() to access []int32 data.
type Vec struct {
	N        uint32         // the number of elements in the bitmap
	Desc     ColumnDesc     // the physical layout of the column
	DataType DataType       // the type of vector elements
	Encoding ColumnEncoding // the encoding of the column data
	Min      uint64         // the minimum integer value in the column
	start    unsafe.Pointer // pointer to start of the column data
	NullBitmap
}

// Bitmap returns the vec data as a Bitmap.
func (v Vec) Bitmap() Bitmap {
	if v.DataType != DataTypeBool {
		panic("vec does not hold bitmap data")
	}
	return Bitmap{
		data:  makeUnsafeRawSlice[uint64](v.start),
		total: int(v.N),
	}
}

// Int32 returns the vec data as []int32. The slice should not be mutated.
func (v Vec) Int32() []int32 {
	if v.DataType != DataTypeUint32 {
		panic("vec does not hold fixed32 data")
	}
	n := v.count(int(v.N))
	return unsafe.Slice((*int32)(v.start), n)
}

// Int64 returns the vec data as []int64. The slice should not be mutated.
func (v Vec) Int64() []int64 {
	if v.DataType != DataTypeUint64 {
		panic("vec does not hold fixed64 data")
	}
	n := v.count(int(v.N))
	return unsafe.Slice((*int64)(v.start), n)
}

// UnsafeInt8s TODO(peter) ...
func (v Vec) UnsafeInt8s() UnsafeInt8s {
	if v.DataType != DataTypeUint8 {
		panic("vec does not hold fixed8 data")
	}
	return makeUnsafeIntegerSlice[int8](0, v.start, 1)
}

// UnsafeUint8s TODO(peter) ...
func (v Vec) UnsafeUint8s() UnsafeUint8s {
	if v.DataType != DataTypeUint8 {
		panic("vec does not hold fixed8 data")
	}
	return makeUnsafeIntegerSlice[uint8](0, v.start, 1)
}

// UnsafeInt16s TODO(peter) ...
func (v Vec) UnsafeInt16s() UnsafeInt16s {
	if v.DataType != DataTypeUint16 {
		panic("vec does not hold fixed16 data")
	}
	return makeUnsafeIntegerSlice[int16](int16(v.Min), v.start, v.Desc.RowWidth())
}

// UnsafeUint16s TODO(peter) ...
func (v Vec) UnsafeUint16s() UnsafeUint16s {
	if v.DataType != DataTypeUint16 {
		panic("vec does not hold fixed16 data")
	}
	return makeUnsafeIntegerSlice[uint16](uint16(v.Min), v.start, v.Desc.RowWidth())
}

// UnsafeInt32s TODO(peter) ...
func (v Vec) UnsafeInt32s() UnsafeInt32s {
	if v.DataType != DataTypeUint32 {
		panic("vec does not hold fixed32 data")
	}
	return makeUnsafeIntegerSlice[int32](int32(v.Min), v.start, v.Desc.RowWidth())
}

// UnsafeUint32s TODO(peter) ...
func (v Vec) UnsafeUint32s() UnsafeUint32s {
	if v.DataType != DataTypeUint32 {
		panic("vec does not hold fixed32 data")
	}
	return makeUnsafeIntegerSlice[uint32](uint32(v.Min), v.start, v.Desc.RowWidth())
}

// UnsafeInt64s TODO(peter) ...
func (v Vec) UnsafeInt64s() UnsafeInt64s {
	if v.DataType != DataTypeUint64 {
		panic("vec does not hold fixed64 data")
	}
	return makeUnsafeIntegerSlice[int64](int64(v.Min), v.start, v.Desc.RowWidth())
}

// UnsafeUint64s TODO(peter) ...
func (v Vec) UnsafeUint64s() UnsafeUint64s {
	if v.DataType != DataTypeUint64 {
		panic("vec does not hold fixed64 data")
	}
	return makeUnsafeIntegerSlice[uint64](v.Min, v.start, v.Desc.RowWidth())
}

// PrefixBytes returns the vec data as PrefixBytes. The underlying data should
// not be mutated.
func (v Vec) PrefixBytes() PrefixBytes {
	if v.DataType != DataTypePrefixBytes {
		panic("vec does not hold bytes data")
	}
	return makePrefixBytes(v.N, v.start)
}

// RawBytes returns the vec data as RawBytes. The underlying data should not
// be mutated.
func (v Vec) RawBytes() RawBytes {
	if v.DataType != DataTypeBytes {
		panic("vec does not hold bytes data")
	}
	return makeRawBytes(v.N, v.start)
}
