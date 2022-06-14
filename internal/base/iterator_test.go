// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func setRandUint64(v reflect.Value) uint64 {
	val := rand.Uint64()
	v.SetUint(val)
	return val
}

func TestInternalIteratorStatsMerge(t *testing.T) {
	var from, to, expected InternalIteratorStats
	n := reflect.ValueOf(from).NumField()
	for i := 0; i < n; i++ {
		switch reflect.ValueOf(from).Type().Field(i).Type.Kind() {
		case reflect.Uint64:
			v1 := setRandUint64(reflect.ValueOf(&from).Elem().Field(i))
			v2 := setRandUint64(reflect.ValueOf(&to).Elem().Field(i))
			reflect.ValueOf(&expected).Elem().Field(i).SetUint(v1 + v2)
		default:
			t.Fatalf("unknown kind %v", reflect.ValueOf(from).Type().Field(i).Type.Kind())
		}
	}
	to.Merge(from)
	require.Equal(t, expected, to)
}

func TestFlags(t *testing.T) {
	t.Run("SeekGEFlags", func(t *testing.T) {
		f := SeekGEFlagsNone
		type flag struct {
			label string
			pred  func() bool
			set   func() SeekGEFlags
			unset func() SeekGEFlags
		}
		flags := []flag{
			{
				"TrySeekUsingNext",
				func() bool { return f.TrySeekUsingNext() },
				func() SeekGEFlags { return f.EnableTrySeekUsingNext() },
				func() SeekGEFlags { return f.DisableTrySeekUsingNext() },
			},
			{
				"RelativeSeek",
				func() bool { return f.RelativeSeek() },
				func() SeekGEFlags { return f.EnableRelativeSeek() },
				func() SeekGEFlags { return f.DisableRelativeSeek() },
			},
		}

		ref := make([]bool, len(flags))
		var checkCombination func(t *testing.T, i int)
		checkCombination = func(t *testing.T, i int) {
			if i >= len(ref) {
				// Verify that ref matches the flag predicates.
				for j := 0; j < i; j++ {
					if got := flags[j].pred(); ref[j] != got {
						t.Errorf("%s() = %t, want %t : flag binary = %08b", flags[j].label, got, ref[j], f)
					}
				}
				return
			}

			// flag i remains unset.
			t.Run(fmt.Sprintf("%s begin unset", flags[i].label), func(t *testing.T) {
				checkCombination(t, i+1)
			})

			// set flag i
			ref[i] = true
			f = flags[i].set()
			t.Run(fmt.Sprintf("%s set", flags[i].label), func(t *testing.T) {
				checkCombination(t, i+1)
			})

			// unset flag i
			ref[i] = false
			f = flags[i].unset()
			t.Run(fmt.Sprintf("%s unset", flags[i].label), func(t *testing.T) {
				checkCombination(t, i+1)
			})
		}
		checkCombination(t, 0)
	})
}
