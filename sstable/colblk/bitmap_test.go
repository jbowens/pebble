// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"
	"unicode"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestBitmapFixed(t *testing.T) {
	var bitmap Bitmap
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/bitmap", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "build":
			var builder bitmapBuilder
			var n int
			for _, r := range td.Input {
				if unicode.IsSpace(r) {
					continue
				}
				if r == '1' {
					builder = builder.Set(n, r == '1')
				}
				n++
			}
			data := make([]byte, bitmapRequiredSize(n))
			_ = builder.Finish(n, 0, data)
			bitmap = Bitmap{
				data:  makeUnsafeRawSlice[uint64](unsafe.Pointer(&data[0])),
				total: n,
			}
			dumpBitmap(&buf, bitmap)
			fmt.Fprint(&buf, "\nBinary representation:\n")
			f := binfmt.New(data)
			bitmapToBinFormatter(f, n)
			fmt.Fprint(&buf, f.String())
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}

func dumpBitmap(w io.Writer, b Bitmap) {
	for i := 0; i < b.total; i++ {
		if i > 0 && i%64 == 0 {
			w.Write([]byte{'\n'})
		}
		if b.Get(i) {
			w.Write([]byte{'1'})
		} else {
			w.Write([]byte{'0'})
		}
	}
}

func TestBitmapRandom(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))
	size := rng.Intn(4096) + 1
	var builder bitmapBuilder
	v := make([]bool, size)
	for i := 0; i < size; i++ {
		v[i] = rng.Intn(2) == 0
		if v[i] {
			builder = builder.Set(i, v[i])
		}
	}
	data := make([]byte, bitmapRequiredSize(size))
	_ = builder.Finish(size, 0, data)
	bitmap := Bitmap{
		data:  makeUnsafeRawSlice[uint64](unsafe.Pointer(&data[0])),
		total: size,
	}
	for i := 0; i < size; i++ {
		j := rng.Intn(size)
		if got := bitmap.Get(j); got != v[j] {
			t.Fatalf("b.Get(%d) = %t; want %t", j, got, v[j])
		}
	}
}

func BenchmarkBitmapBuilder(b *testing.B) {
	seed := uint64(10024282523)
	rng := rand.New(rand.NewSource(seed))
	size := rng.Intn(4096) + 1
	v := make([]bool, size)
	for i := 0; i < size; i++ {
		v[i] = rng.Intn(2) == 0
	}
	data := make([]byte, bitmapRequiredSize(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var builder bitmapBuilder
		for i := 0; i < size; i++ {
			if v[i] {
				builder = builder.Set(i, v[i])
			}
		}
		_ = builder.Finish(size, 0, data)
	}
}

// FinishAlloc allocates and returns a copy of the bitmap on the heap.
func (b nullBitmapBuilder) FinishAlloc() NullBitmap {
	nb := make([]byte, b.Size(0))
	nb = nb[:b.Finish(0, nb)]
	return NullBitmap{data: makeUnsafeRawSlice[nullBitmapWord](unsafe.Pointer(unsafe.SliceData(nb)))}
}

func TestNullBitmapFixed(t *testing.T) {
	var n int
	var bitmap NullBitmap
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/null_bitmap", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "build":
			var builder nullBitmapBuilder
			n = 0
			for _, r := range td.Input {
				if unicode.IsSpace(r) {
					continue
				}
				builder = builder.Set(n, r == '1')
				n++
			}
			b := make([]byte, builder.Size(0))
			b = b[:builder.Finish(0, b)]
			f := binfmt.New(b)
			nullBitmapToBinFormatter(f, n)
			fmt.Fprint(&buf, f.String())
			return buf.String()
		case "rank":
			for i, cmdArg := range td.CmdArgs {
				if i > 0 {
					buf.WriteRune(' ')
				}
				v, err := strconv.Atoi(cmdArg.Key)
				require.NoError(t, err)
				fmt.Fprintf(&buf, "%d", bitmap.Rank(v))
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}

func randNullBitmap(rng *rand.Rand, size int) NullBitmap {
	var builder nullBitmapBuilder
	for i := 0; i < size; i++ {
		builder = builder.Set(i, rng.Intn(2) == 0)
	}
	return makeNullBitmap(builder)
}

func TestNullBitmapRandom(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))
	size := rng.Intn(4096) + 1
	var builder nullBitmapBuilder
	ref := make([]bool, size)
	rank := make([]int, size)
	var nonNull int
	for i := 0; i < size; i++ {
		ref[i] = rng.Intn(2) == 0
		builder = builder.Set(i, ref[i])
		if ref[i] {
			rank[i] = -1
		} else {
			rank[i] = nonNull
			nonNull++
		}
	}
	b := builder.FinishAlloc()
	for i := 0; i < size; i++ {
		j := rng.Intn(size)
		if got := b.Null(j); got != ref[j] {
			t.Fatalf("b.Null(%d) = %t; want %t", j, got, ref[j])
		}
		if got := b.Rank(j); got != rank[j] {
			t.Fatalf("b.Rank(%d) = %d; want %d", j, got, rank[j])
		}
	}
}

func BenchmarkNullBitmapGet(b *testing.B) {
	const size = 4096
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	bmap := randNullBitmap(rng, size)
	b.ResetTimer()

	var sum int
	for i, k := 0, 0; i < b.N; i += k {
		for j := 0; j < min(size, b.N-i); j++ {
			if bmap.Null(j) {
				sum++
			}
		}
	}

	b.StopTimer()
	fmt.Fprint(io.Discard, sum)
}

func BenchmarkNullBitmapRank(b *testing.B) {
	const size = 4096
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	bmap := randNullBitmap(rng, size)
	b.ResetTimer()

	var sum int
	for i, k := 0, 0; i < b.N; i += k {
		for j := 0; j < min(size, b.N-i); j++ {
			if r := bmap.Rank(j); r >= 0 {
				sum++
			}
		}
	}

	b.StopTimer()
	fmt.Fprint(io.Discard, sum)
}
func BenchmarkNullBitmapBuilder(b *testing.B) {
	seed := uint64(10024282523)
	rng := rand.New(rand.NewSource(seed))
	size := rng.Intn(4096) + 1
	v := make([]bool, size)
	for i := 0; i < size; i++ {
		v[i] = rng.Intn(2) == 0
	}
	data := make([]byte, nullBitmapSize(0, size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var builder nullBitmapBuilder
		for i := 0; i < size; i++ {
			if v[i] {
				builder = builder.Set(i, v[i])
			}
		}
		_ = builder.Finish(0, data)
	}
}
