// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package whiteboard

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/strparse"
)

func TestASCIIBoard(t *testing.T) {
	var board ASCIIBoard
	datadriven.RunTest(t, "testdata/ascii_board", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "make":
			var w, h int
			td.ScanArgs(t, "w", &w)
			td.ScanArgs(t, "h", &h)
			board = Make(w, h)
			return board.String()
		case "write":
			for _, line := range strings.Split(td.Input, "\n") {
				p := strparse.MakeParser(" ", line)
				r := p.Int()
				c := p.Int()
				board.At(r, c).WriteString(p.Remaining())
			}
			return board.String()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
