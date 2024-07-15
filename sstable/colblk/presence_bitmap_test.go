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

// FinishAlloc allocates and returns a copy of the bitmap on the heap.
func (b presenceBitmapBuilder) FinishAlloc() PresenceBitmap {
	nb := make([]byte, b.Size(0))
	nb = nb[:b.Finish(0, nb)]
	return PresenceBitmap{data: makeUnsafeRawSlice[presenceBitmapWord](unsafe.Pointer(unsafe.SliceData(nb)))}
}

func TestPresenceBitmapFixed(t *testing.T) {
	var n int
	var bitmap PresenceBitmap
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/presence_bitmap", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "build":
			var builder presenceBitmapBuilder
			n = 0
			for _, r := range td.Input {
				if unicode.IsSpace(r) {
					continue
				}
				builder.Set(n, r == '1')
				n++
			}
			b := make([]byte, builder.Size(0))
			b = b[:builder.Finish(0, b)]
			f := binfmt.New(b)
			presenceBitmapToBinFormatter(f, n)
			fmt.Fprint(&buf, f.String())
			bitmap = PresenceBitmap{data: makeUnsafeRawSlice[presenceBitmapWord](unsafe.Pointer(unsafe.SliceData(b)))}
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

func randPresenceBitmap(rng *rand.Rand, size int) PresenceBitmap {
	var builder presenceBitmapBuilder
	for i := 0; i < size; i++ {
		builder.Set(i, rng.Intn(2) == 0)
	}
	return makePresenceBitmap(builder.words)
}

func TestPresenceBitmapRandom(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))
	size := rng.Intn(4096) + 1
	var builder presenceBitmapBuilder
	ref := make([]bool, size)
	rank := make([]int, size)
	var present int
	for i := 0; i < size; i++ {
		ref[i] = rng.Intn(2) == 0
		builder.Set(i, ref[i])
		if !ref[i] {
			rank[i] = -1
			continue
		}
		rank[i] = present
		present++
	}
	b := builder.FinishAlloc()
	perm := rng.Perm(size)
	for _, j := range perm {
		if got := b.Present(j); got != ref[j] {
			t.Fatalf("b.Present(%d) = %t; want %t", j, got, ref[j])
		}
		if got := b.Rank(j); got != rank[j] {
			t.Fatalf("b.Rank(%d) = %d; want %d", j, got, rank[j])
		}
	}
}

func BenchmarkPresenceBitmapGet(b *testing.B) {
	const size = 4096
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	bmap := randPresenceBitmap(rng, size)
	b.ResetTimer()

	var sum int
	for i, k := 0, 0; i < b.N; i += k {
		for j := 0; j < min(size, b.N-i); j++ {
			if bmap.Present(j) {
				sum++
			}
		}
	}

	b.StopTimer()
	fmt.Fprint(io.Discard, sum)
}

func BenchmarkPresenceBitmapRank(b *testing.B) {
	const size = 4096
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	bmap := randPresenceBitmap(rng, size)
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
func BenchmarkPresenceBitmapBuilder(b *testing.B) {
	seed := uint64(10024282523)
	rng := rand.New(rand.NewSource(seed))
	size := rng.Intn(4096) + 1
	v := make([]bool, size)
	for i := 0; i < size; i++ {
		v[i] = rng.Intn(2) == 0
	}
	data := make([]byte, presenceBitmapSize(0, size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var builder presenceBitmapBuilder
		for i := 0; i < size; i++ {
			if v[i] {
				builder.Set(i, v[i])
			}
		}
		_ = builder.Finish(0, data)
	}
}
