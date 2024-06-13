// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"encoding/binary"
	"math/bits"
)

// Uvarint32 holds a column of 32-bit integers, stored as variable-length group
// varints of 4. Each varint group is stored as a slice within a RawBytes type.
// RawBytes provides constant-time access to the bundle of 4 integers. When all
// four integers within a group are zero, the encoded varint is elided,
// represented as an empty slice within RawBytes. This means that a column of
// all zeros requires one 16-bit offset per group (4 bits per uint32).
//
// The []byte{0x00} value is never used (because we elide the byte slice if all
// of the int values are zero). We could possibly use it to represent an
// alternative state.
type Uvarint32 struct {
	varints RawBytes
}

// Get returns the i-th value in the column.
func (u *Uvarint32) Get(i int) uint32 {
	grp := u.varints.At(i / 4)
	if len(grp) == 0 {
		return 0
	}
	// TODO(jackson): Avoid unnecessarily decoding the entire group.
	var dst [4]uint32
	decodeGroupVarint32(dst[:], grp)
	return dst[i%4]
}

const maxVarint32Len = 17

type uvarint32Builder struct {
	bb  BytesBuilder
	grp [4]uint32
	buf [maxVarint32Len]byte
	n   uint32
}

func (vb *uvarint32Builder) Reset() {
	vb.bb.Reset()
	vb.n = 0
}

func (vb *uvarint32Builder) Append(v uint32) {
	vb.grp[vb.n%4] = v
	vb.n++
	if vb.n%4 == 0 {
		if vb.grp == [4]uint32{} {
			vb.bb.Put(nil)
		} else {
			vb.bb.Put(encodeGroupVarint32(vb.buf[:], vb.grp[:]))
		}
	}
}

func (vb *uvarint32Builder) MaxSize(offset uint32) uint32 {
	// TODO(jackson): Tighten the bound on the current pending group.
	return vb.bb.Size(int((vb.n+3)/4), offset) + maxVarint32Len + align32
}

func (vb *uvarint32Builder) Finish(offset uint32, buf []byte) uint32 {
	if i := vb.n % 4; i > 0 {
		for j := i; j < 4; j++ {
			vb.grp[j] = 0
		}
		if vb.grp == [4]uint32{} {
			vb.bb.Put(nil)
		} else {
			vb.bb.Put(encodeGroupVarint32(vb.buf[:], vb.grp[:]))
		}
	}
	off, _ := vb.bb.Finish(0, int((vb.n+3)/4), offset, buf)
	return off
}

func encodeGroupVarint32(dst []byte, src []uint32) []byte {
	var nbits uint8
	var n, b uint32

	offs := uint32(1)

	n = src[0]
	binary.LittleEndian.PutUint32(dst[offs:], n)
	b = 3 - uint32(bits.LeadingZeros32(n|1)/8)
	nbits |= byte(b)
	offs += b + 1

	n = src[1]
	binary.LittleEndian.PutUint32(dst[offs:], n)
	b = 3 - uint32(bits.LeadingZeros32(n|1)/8)
	nbits |= byte(b) << 2
	offs += b + 1

	n = src[2]
	binary.LittleEndian.PutUint32(dst[offs:], n)
	b = 3 - uint32(bits.LeadingZeros32(n|1)/8)
	nbits |= byte(b) << 4
	offs += b + 1

	n = src[3]
	binary.LittleEndian.PutUint32(dst[offs:], n)
	b = 3 - uint32(bits.LeadingZeros32(n|1)/8)
	nbits |= byte(b) << 6
	offs += b + 1

	dst[0] = nbits

	return dst[:offs]
}

var mask = [4]uint32{0xff, 0xffff, 0xffffff, 0xffffffff}

func decodeGroupVarint32(dst []uint32, src []byte) {
	nbits := src[0]
	src = src[1:]

	b := nbits & 3
	n := loadVarint32(src, mask[b])
	src = src[1+b:]
	nbits >>= 2
	dst[0] = uint32(n)

	b = nbits & 3
	n = loadVarint32(src, mask[b])
	src = src[1+b:]
	nbits >>= 2
	dst[1] = uint32(n)

	b = nbits & 3
	n = loadVarint32(src, mask[b])
	src = src[1+b:]
	nbits >>= 2
	dst[2] = uint32(n)

	b = nbits & 3
	n = loadVarint32(src, mask[b])
	dst[3] = uint32(n)
}

func loadVarint32(src []byte, mask uint32) uint32 {
	if len(src) > 4 {
		return binary.LittleEndian.Uint32(src) & mask
	}
	switch mask {
	case 0xff:
		return uint32(src[0])
	case 0xffff:
		return uint32(binary.LittleEndian.Uint16(src))
	case 0xffffff:
		return uint32(binary.LittleEndian.Uint16(src)) | uint32(src[2])<<16
	case 0xffffffff:
		return binary.LittleEndian.Uint32(src)
	}
	return 0
}
