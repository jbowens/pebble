// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import "encoding/binary"

// EncodedValueLen ...
//
// TODO(jackson): document.
func EncodedValueLen(endKey, value []byte) int {
	x := uint32(len(endKey))
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n + len(endKey) + len(value)
}

// EncodeValue ...
//
// TODO(jackson): document.
func EncodeValue(dst, endKey, value []byte) int {
	n := binary.PutUvarint(dst, uint64(len(endKey)))
	copy(dst[n:], endKey)
	copy(dst[n+len(endKey):], value)
	return n + len(endKey) + len(value)
}
