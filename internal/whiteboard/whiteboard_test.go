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
				x := p.Int()
				y := p.Int()
				board.At(x, y).WriteString(p.Remaining())
			}
			return board.String()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
