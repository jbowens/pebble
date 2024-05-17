// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"unsafe"

	"golang.org/x/exp/constraints"
)

type UnsafeInt8s = UnsafeIntegerSlice[int8]
type UnsafeInt16s = UnsafeIntegerSlice[int16]
type UnsafeInt32s = UnsafeIntegerSlice[int32]
type UnsafeInt64s = UnsafeIntegerSlice[int64]
type UnsafeUint8s = UnsafeIntegerSlice[uint8]
type UnsafeUint16s = UnsafeIntegerSlice[uint16]
type UnsafeUint32s = UnsafeIntegerSlice[uint32]
type UnsafeUint64s = UnsafeIntegerSlice[uint64]

// UnsafeRawSlice maintains a pointer to a slice of elements of type T.
// UnsafeRawSlice provides no bounds checking.
type UnsafeRawSlice[T constraints.Integer] struct {
	ptr unsafe.Pointer
}

func makeUnsafeRawSlice[T constraints.Integer](ptr unsafe.Pointer) UnsafeRawSlice[T] {
	// TODO(peter): verify alignment
	return UnsafeRawSlice[T]{ptr: ptr}
}

// At returns the `i`-th element of the slice.
func (s UnsafeRawSlice[T]) At(i int) T {
	return *(*T)(unsafe.Pointer(uintptr(s.ptr) + unsafe.Sizeof(T(0))*uintptr(i)))
}

// Slice returns a go []T slice containing the first `len` elements of the
// unsafe slice.
func (s UnsafeRawSlice[T]) Slice(len int) []T {
	return unsafe.Slice((*T)(s.ptr), len)
}

// set mutates the slice, setting the `i`-th value to `v`.
func (s UnsafeRawSlice[T]) set(i int, v T) {
	*(*T)(unsafe.Pointer(uintptr(s.ptr) + unsafe.Sizeof(T(0))*uintptr(i))) = v
}

// UnsafeIntegerSlice exposes a read-only slice of integers from a column. If
// the column's values are delta-encoded, IntegerSlice transparently applies
// deltas.
type UnsafeIntegerSlice[T constraints.Integer] struct {
	base       T
	deltaPtr   unsafe.Pointer
	deltaWidth uintptr
}

func makeUnsafeIntegerSlice[T constraints.Integer](
	base T, deltaPtr unsafe.Pointer, deltaWidth int,
) UnsafeIntegerSlice[T] {
	return UnsafeIntegerSlice[T]{
		base:       base,
		deltaPtr:   deltaPtr,
		deltaWidth: uintptr(deltaWidth),
	}
}

// At returns the `i`-th element of the slice.
func (s UnsafeIntegerSlice[T]) At(i int) T {
	switch s.deltaWidth {
	case 0:
		return s.base
	case 1:
		return s.base + T(*(*int8)(unsafe.Pointer(uintptr(s.deltaPtr) + uintptr(i))))
	case 2:
		return s.base + T(*(*int16)(unsafe.Pointer(uintptr(s.deltaPtr) + 2*uintptr(i))))
	case 4:
		return s.base + T(*(*int32)(unsafe.Pointer(uintptr(s.deltaPtr) + 4*uintptr(i))))
	case 8:
		// NB: The slice encodes 64-bit integers, there is no base (it doesn't
		// save any bits to compute a delta) and T must be a 64-bit integer. We
		// cast directly into a *T pointer and don't add the base.
		return (*(*T)(unsafe.Pointer(uintptr(s.deltaPtr) + 8*uintptr(i))))
	default:
		panic("unreachable")
	}
}

// Clone allocates a new slice and copies the first `rows` elements.
func (s UnsafeIntegerSlice[T]) Clone(rows int) []T {
	result := make([]T, rows)
	for i := 0; i < rows; i++ {
		result[i] = s.At(i)
	}
	return result
}
