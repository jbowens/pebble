// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestColumnDesc(t *testing.T) {
	// Ensure that a simple roundtrip from DataType -> ColumnDesc ->
	// DataType arrives back at the original DataType.
	for dt := DataTypeInvalid; dt < dataTypesCount; dt++ {
		require.Equal(t, dt, ColumnDesc(dt).DataType())
	}

	type testCase struct {
		desc       ColumnDesc
		dataType   DataType
		enc        ColumnEncoding
		nullBitmap bool
		str        string
	}
	testCases := []testCase{
		{
			desc:       ColumnDesc(DataTypeBool),
			dataType:   DataTypeBool,
			str:        "bool",
			nullBitmap: false,
			enc:        EncodingDefault,
		},
		{
			desc:       ColumnDesc(DataTypeUint16),
			dataType:   DataTypeUint16,
			enc:        EncodingDefault,
			nullBitmap: false,
			str:        "int16",
		},
		{
			desc:       ColumnDesc(DataTypeUint64).WithEncoding(EncodingConstant).WithNullBitmap(),
			dataType:   DataTypeUint64,
			enc:        EncodingConstant,
			nullBitmap: true,
			str:        "int64+nullbitmap+constant",
		},
		{
			desc:       ColumnDesc(DataTypePrefixBytes),
			dataType:   DataTypePrefixBytes,
			nullBitmap: false,
			str:        "prefixbytes",
		},
		{
			desc:       ColumnDesc(DataTypeBytes).WithEncoding(EncodingConstant),
			dataType:   DataTypeBytes,
			enc:        EncodingConstant,
			nullBitmap: false,
			str:        "bytes+constant",
		},
		{
			desc:       ColumnDesc(DataTypeBytes),
			dataType:   DataTypeBytes,
			enc:        EncodingDefault,
			nullBitmap: false,
			str:        "bytes",
		},
		{
			desc:       ColumnDesc(DataTypeUint64).WithEncoding(EncodingDeltaInt16),
			dataType:   DataTypeUint64,
			enc:        EncodingDeltaInt16,
			nullBitmap: false,
			str:        "int64+delta16",
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%08b", tc.desc), func(t *testing.T) {
			if got := tc.desc.String(); got != tc.str {
				t.Errorf("String() = %q; want %q", got, tc.str)
			}
			if got := tc.desc.DataType(); got != tc.dataType {
				t.Errorf("DataType() = %s; want %s", got, tc.dataType)
			}
			if got := tc.desc.Encoding(); got != tc.enc {
				t.Errorf("Encoding() = %s; want %s", got, tc.enc)
			}
			if got := tc.desc.HasNullBitmap(); got != tc.nullBitmap {
				t.Errorf("HasNullBitmap() = %t; want %t", got, tc.nullBitmap)
			}
		})
	}
}

func TestPrefixCompress(t *testing.T) {
	var data bytes.Buffer
	var out bytes.Buffer
	datadriven.RunTest(t, "testdata/prefix_compress", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "compress":
			data.Reset()
			out.Reset()

			var bundleSize int
			td.ScanArgs(t, "bundle-size", &bundleSize)
			var b bytesBuilder
			b.Init(bundleSize)
			for _, r := range td.Input {
				switch r {
				case '|':
					b.addOffset(uint32(data.Len()))
				case ' ', '\n':
					continue
				default:
					data.WriteRune(r)
				}
			}
			b.currentBundlePrefixOffset = 1 + (b.bundleSize+1)*(max(0, (len(b.offsets)-3))>>b.bundleShift)
			b.data = data.Bytes()
			fmt.Fprintln(&out, b.debugString(0))
			fmt.Fprintln(&out, "-")
			b.prefixCompress()
			wrapStr(&out, string(b.data), 80)
			return out.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}

func TestRawBytes(t *testing.T) {
	var out bytes.Buffer
	var builder bytesBuilder
	datadriven.RunTest(t, "testdata/raw_bytes", func(t *testing.T, td *datadriven.TestData) string {
		out.Reset()
		switch td.Cmd {
		case "build":
			builder.Init(0 /* bundleSize */)

			var startOffset int
			td.ScanArgs(t, "offset", &startOffset)
			td.MaybeScanArgs(t, "max-offset-16", &builder.maxOffset16)

			var count int
			for _, k := range strings.Split(strings.TrimSpace(td.Input), "\n") {
				builder.Put([]byte(k))
				count++
			}

			size := builder.Size(count, uint32(startOffset))
			fmt.Fprintf(&out, "Size: %d\n", size-uint32(startOffset))

			buf := make([]byte, uint32(startOffset)+size+align64)
			endOffset, _ := builder.Finish(count, uint32(startOffset), buf)

			// Validate that builder.Size() was correct in its estimate.
			require.Equal(t, size, endOffset)
			f := binfmt.New(buf).LineWidth(20)
			f.HexBytesln(startOffset, "start offset")
			rawBytesToBinFormatter(f, uint32(count), nil)
			return f.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}

func TestPrefixBytes(t *testing.T) {
	var out bytes.Buffer
	var pb PrefixBytes
	var builder bytesBuilder
	var keys int
	var getBuf []byte

	datadriven.RunTest(t, "testdata/prefix_bytes", func(t *testing.T, td *datadriven.TestData) string {
		out.Reset()
		switch td.Cmd {
		case "init":
			var bundleSize int
			td.ScanArgs(t, "bundle-size", &bundleSize)
			builder.Init(bundleSize)
			td.MaybeScanArgs(t, "max-offset-16", &builder.maxOffset16)

			keys = 0
			fmt.Fprintf(&out, "Size: %d", builder.Size(keys, 0))
			return out.String()
		case "put":
			for _, k := range strings.Split(strings.TrimSpace(td.Input), "\n") {
				builder.PutOrdered([]byte(k))
				keys++
			}
			fmt.Fprint(&out, builder.debugString(0))
			return out.String()
		case "finish":
			buf := make([]byte, builder.Size(keys, 0))
			offset, _ := builder.Finish(keys, 0, buf)
			require.Equal(t, uint32(len(buf)), offset)
			hexDump(&out, buf)
			fmt.Fprintln(&out)

			pb = makePrefixBytes(uint32(builder.nKeys), unsafe.Pointer(unsafe.SliceData(buf)))
			fmt.Fprint(&out, pb.DebugString())
			return out.String()
		case "get":
			var indices []int
			td.ScanArgs(t, "indices", &indices)

			getBuf = append(getBuf[:0], pb.SharedPrefix()...)
			l := len(getBuf)
			for _, i := range indices {
				getBuf = append(append(getBuf[:l], pb.BundlePrefix(i)...), pb.RowSuffix(i)...)
				fmt.Fprintf(&out, "%s\n", getBuf)
			}
			return out.String()

		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}

func TestPrefixBytesRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("Seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	minLen := randInt(4, 128)
	maxLen := randInt(4, 128)
	if maxLen < minLen {
		minLen, maxLen = maxLen, minLen
	}
	blockPrefixLen := randInt(1, minLen+1)
	userKeys := make([][]byte, randInt(1, 10000))
	// Create the first user key.
	userKeys[0] = make([]byte, randInt(minLen, maxLen+1))
	for j := range userKeys[0] {
		userKeys[0][j] = byte(randInt(int('a'), int('z')+1))
	}
	// Create the remainder of the user keys, giving them the same block prefix
	// of length [blockPrefixLen].
	for i := 1; i < len(userKeys); i++ {
		userKeys[i] = make([]byte, randInt(minLen, maxLen+1))
		copy(userKeys[i], userKeys[0][:blockPrefixLen])
		for j := blockPrefixLen; j < len(userKeys[i]); j++ {
			userKeys[i][j] = byte(randInt(int('a'), int('z')+1))
		}
	}
	slices.SortFunc(userKeys, bytes.Compare)

	var bb bytesBuilder
	bb.Init(1 << randInt(1, 4))
	for i := 0; i < len(userKeys); i++ {
		bb.PutOrdered(userKeys[i])
	}

	//t.Log(bb.debugString(0, data))

	size := bb.Size(len(userKeys), 0)
	buf := make([]byte, size)
	offset, _ := bb.Finish(len(userKeys), 0, buf)
	t.Logf("%d 16-bit offsets; %d 32-bit offsets", bb.nOffsets16, len(bb.offsets)-int(bb.nOffsets16))

	if uint32(size) != offset {
		t.Fatalf("bb.Size(...) computed %d, but bb.Finish(...) produced slice of len %d (%d is string data)",
			size, offset, len(bb.data))
	}

	require.Equal(t, uint32(len(buf)), offset)

	pb := makePrefixBytes(uint32(bb.nKeys), unsafe.Pointer(unsafe.SliceData(buf)))
	// t.Log("Prefix bytes table:\n" + pb.DebugString())

	k := append([]byte(nil), pb.SharedPrefix()...)
	l := len(k)
	for i := 0; i < min(10000, len(userKeys)); i++ {
		j := rand.Intn(len(userKeys))
		k = append(append(k[:l], pb.BundlePrefix(j)...), pb.RowSuffix(j)...)
		if !bytes.Equal(k, userKeys[j]) {
			t.Fatalf("Constructed key %q (%q, %q, %q) for index %d; expected %q",
				k, pb.SharedPrefix(), pb.BundlePrefix(j), pb.RowSuffix(j), j, userKeys[j])
		}
	}
}

func dataTypeFromName(name string) DataType {
	for dt, n := range dataTypeName {
		if n == name {
			return DataType(dt)
		}
	}
	return DataTypeInvalid
}

func hexDump(w io.Writer, b []byte) {
	for len(b) > 0 {
		n := min(32, len(b))
		l := b[:n]
		b = b[n:]
		fmt.Fprintf(w, "%x", l)
		if len(b) > 0 {
			fmt.Fprintln(w)
		}
	}
}

func wrapStr(w io.Writer, s string, width int) {
	for len(s) > 0 {
		n := min(width, len(s))
		fmt.Fprint(w, s[:n])
		s = s[n:]
		if len(s) > 0 {
			fmt.Fprintln(w)
		}
	}
}

func (b *bytesBuilder) debugString(offset uint32) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Size: %d", b.Size(b.nKeys, offset))
	fmt.Fprintf(&sb, "\nnKeys=%d; bundleSize=%d; nOffsets16=%d", b.nKeys, b.bundleSize, b.nOffsets16)
	if b.bundleSize > 0 {
		fmt.Fprintf(&sb, "\nnBundles=%d; blockPrefixLen=%d; currentBundleLen=%d; currentBundleKeys=%d",
			b.nBundles, b.blockPrefixLen, b.currentBundleLen, b.currentBundleKeys)
	}
	fmt.Fprint(&sb, "\nOffsets:")
	for i := range b.offsets {
		if i%10 == 0 {
			fmt.Fprintf(&sb, "\n  %04d", b.offsets[i])
		} else {
			fmt.Fprintf(&sb, "  %04d", b.offsets[i])
		}
	}
	fmt.Fprintf(&sb, "\nData (len=%d):\n", len(b.data))
	wrapStr(&sb, string(b.data), 60)
	return sb.String()
}

func BenchmarkPrefixBytes(b *testing.B) {
	seed := uint64(205295296)
	rng := rand.New(rand.NewSource(seed))
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	minLen := 8
	maxLen := 128
	if maxLen < minLen {
		minLen, maxLen = maxLen, minLen
	}
	blockPrefixLen := 6
	userKeys := make([][]byte, 1000)
	// Create the first user key.
	userKeys[0] = make([]byte, randInt(minLen, maxLen+1))
	for j := range userKeys[0] {
		userKeys[0][j] = byte(randInt(int('a'), int('z')+1))
	}
	// Create the remainder of the user keys, giving them the same block prefix
	// of length [blockPrefixLen].
	for i := 1; i < len(userKeys); i++ {
		userKeys[i] = make([]byte, randInt(minLen, maxLen+1))
		copy(userKeys[i], userKeys[0][:blockPrefixLen])
		for j := blockPrefixLen; j < len(userKeys[i]); j++ {
			userKeys[i][j] = byte(randInt(int('a'), int('z')+1))
		}
	}
	slices.SortFunc(userKeys, bytes.Compare)

	var bb bytesBuilder
	var buf []byte
	build := func(n int) []byte {
		bb.Init(16)
		for i := 0; i < n; i++ {
			bb.PutOrdered(userKeys[i])
		}
		size := bb.Size(n, 0)
		if cap(buf) < int(size) {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}
		_, _ = bb.Finish(n, 0, buf)
		return buf
	}

	b.Run("building", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data := build(len(userKeys))
			fmt.Fprint(io.Discard, data)
		}
	})

	b.Run("iteration", func(b *testing.B) {
		n := len(userKeys)
		buf = build(n)
		pb := makePrefixBytes(uint32(n), unsafe.Pointer(unsafe.SliceData(buf)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := append([]byte(nil), pb.SharedPrefix()...)
			l := len(k)
			for i := 0; i < n; i++ {
				j := rand.Intn(n)
				k = append(append(k[:l], pb.BundlePrefix(j)...), pb.RowSuffix(j)...)
				if !bytes.Equal(k, userKeys[j]) {
					b.Fatalf("Constructed key %q (%q, %q, %q) for index %d; expected %q",
						k, pb.SharedPrefix(), pb.BundlePrefix(j), pb.RowSuffix(j), j, userKeys[j])
				}
			}
		}
	})
}
