// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/stretchr/testify/require"
)

func TestColumnarWriter(t *testing.T) {
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/columnar_writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "build":
			var opts WriterOptions
			opts.KeySchema = colblk.DefaultKeySchema(testkeys.Comparer, 4)
			opts.ensureDefaults()
			require.NoError(t, optsFromArgs(td, &opts))
			o := &objstorage.MemObj{}
			w := NewColumnarWriter(o, opts)

			for _, line := range strings.Split(td.Input, "\n") {
				j := strings.Index(line, ":")
				key := base.ParseInternalKey(line[:j])
				value := []byte(line[j+1:])
				if err := w.AddWithForceObsolete(key, value, false); err != nil {
					return err.Error()
				}
			}

			if err := w.Close(); err != nil {
				return err.Error()
			}
			meta, err := w.Metadata()
			require.NoError(t, err)
			return formatMetadata(td, meta)
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}
