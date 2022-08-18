//go:build !invariants
// +build !invariants

package dbg

import "io"

// On is true if we were built with the "debug" build tag.
const On = false

var Sink io.Writer = nil

func Logf(format string, args ...interface{}) {
}
