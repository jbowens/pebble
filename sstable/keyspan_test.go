package sstable

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func BenchmarkKeyspanBlock_RangeDeletions(b *testing.B) {
	for _, numSpans := range []int{1, 10, 100} {
		for _, keysPerSpan := range []int{1, 2, 5} {
			for _, keySize := range []int{10, 128} {
				b.Run(fmt.Sprintf("numSpans=%d/keysPerSpan=%d/keySize=%d", numSpans, keysPerSpan, keySize), func(b *testing.B) {
					benchmarkKeyspanBlockRangeDeletions(b, numSpans, keysPerSpan, keySize)
				})
			}
		}
	}
}

func benchmarkKeyspanBlockRangeDeletions(b *testing.B, numSpans, keysPerSpan, keySize int) {
	var w rowblk.Writer
	format := fmt.Sprintf("%%0%dd", keySize)
	keys := make([][]byte, numSpans+1)
	for k := range keys {
		keys[k] = fmt.Appendf([]byte{}, format, k)
	}
	for i := 0; i < numSpans; i++ {
		for j := 0; j < keysPerSpan; j++ {
			w.Add(base.MakeInternalKey(keys[i], base.SeqNum(j), base.InternalKeyKindRangeDelete), keys[i+1])
		}

	}
	var bp block.BufferPool
	bp.Init(10 << 20)
	defer bp.Release()

	blockData := w.Finish()
	buf := block.Alloc(len(blockData), &bp)
	defer buf.Release()
	copy(buf.Get(), blockData)
	avgRowSize := float64(len(blockData)) / float64(numSpans*keysPerSpan)

	h := buf.MakeHandle(nil, 0, 0, 0)
	comparer := base.DefaultComparer
	var transforms block.FragmentIterTransforms
	it, err := rowblk.NewFragmentIter(0, comparer.Compare, comparer.Split, h, transforms)
	require.NoError(b, err)
	defer it.Close()
	b.Run("SeekGE", func(b *testing.B) {
		rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

		b.ResetTimer()
		var sum uint64
		for i := 0; i < b.N; i++ {
			if s, _ := it.SeekGE(keys[rng.Intn(len(keys))]); s != nil {
				for _, k := range s.Keys {
					sum += uint64(k.Trailer)
				}
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
		fmt.Fprint(io.Discard, sum)
	})
	b.Run("Next", func(b *testing.B) {
		_, _ = it.First()
		b.ResetTimer()
		var sum uint64
		for i := 0; i < b.N; i++ {
			s, _ := it.Next()
			if s == nil {
				s, _ = it.First()
			}
			for _, k := range s.Keys {
				sum += uint64(k.Trailer)
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
		fmt.Fprint(io.Discard, sum)
	})
}
