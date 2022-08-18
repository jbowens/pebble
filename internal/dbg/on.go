//go:build invariants || race
// +build invariants race

package dbg

import (
	"fmt"
	"io"
	"os"
)

// On is true if we were built with the "debug" build tag.
const On = true

var Sink io.Writer = os.Stderr

func Logf(format string, args ...interface{}) {
	fmt.Fprintf(Sink, "  ! "+format+"\n", args...)
}
