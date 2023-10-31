// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package itertest

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/stretchr/testify/require"
)

type iterCmdOpts struct {
	verboseKey bool
	stats      *base.InternalIteratorStats
}

// An IterOpt configures the behavior of RunInternalIterCmd.
type IterOpt func(*iterCmdOpts)

// Verbose configures RunInternalIterCmd to output verbose results.
func Verbose(opts *iterCmdOpts) { opts.verboseKey = true }

// WithStats configures RunInternalIterCmd to collect iterator stats in the
// struct pointed to by stats.
func WithStats(stats *base.InternalIteratorStats) IterOpt {
	return func(opts *iterCmdOpts) {
		opts.stats = stats
	}
}

// RunInternalIterCmd evaluates a datadriven command controlling an internal
// iterator, returning a string with the results of the iterator operations.
func RunInternalIterCmd(
	t *testing.T, d *datadriven.TestData, iter base.InternalIterator, opts ...IterOpt,
) string {
	var o iterCmdOpts
	for _, opt := range opts {
		opt(&o)
	}

	getKV := func(key *base.InternalKey, val base.LazyValue) (*base.InternalKey, []byte) {
		v, _, err := val.Value(nil)
		require.NoError(t, err)
		return key, v
	}
	var b bytes.Buffer
	var prefix []byte
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		var key *base.InternalKey
		var value []byte
		switch parts[0] {
		case "seek-ge":
			if len(parts) < 2 || len(parts) > 3 {
				return "seek-ge <key> [<try-seek-using-next>]\n"
			}
			prefix = nil
			var flags base.SeekGEFlags
			if len(parts) == 3 {
				if trySeekUsingNext, err := strconv.ParseBool(parts[2]); err != nil {
					return err.Error()
				} else if trySeekUsingNext {
					flags = flags.EnableTrySeekUsingNext()
				}
			}
			key, value = getKV(iter.SeekGE([]byte(strings.TrimSpace(parts[1])), flags))
		case "seek-prefix-ge":
			if len(parts) != 2 && len(parts) != 3 {
				return "seek-prefix-ge <key> [<try-seek-using-next>]\n"
			}
			prefix = []byte(strings.TrimSpace(parts[1]))
			var flags base.SeekGEFlags
			if len(parts) == 3 {
				if trySeekUsingNext, err := strconv.ParseBool(parts[2]); err != nil {
					return err.Error()
				} else if trySeekUsingNext {
					flags = flags.EnableTrySeekUsingNext()
				}
			}
			key, value = getKV(iter.SeekPrefixGE(prefix, prefix /* key */, flags))
		case "seek-lt":
			if len(parts) != 2 {
				return "seek-lt <key>\n"
			}
			prefix = nil
			key, value = getKV(iter.SeekLT([]byte(strings.TrimSpace(parts[1])), base.SeekLTFlagsNone))
		case "first":
			prefix = nil
			key, value = getKV(iter.First())
		case "last":
			prefix = nil
			key, value = getKV(iter.Last())
		case "next":
			key, value = getKV(iter.Next())
		case "prev":
			key, value = getKV(iter.Prev())
		case "set-bounds":
			if len(parts) <= 1 || len(parts) > 3 {
				return "set-bounds lower=<lower> upper=<upper>\n"
			}
			var lower []byte
			var upper []byte
			for _, part := range parts[1:] {
				arg := strings.Split(strings.TrimSpace(part), "=")
				switch arg[0] {
				case "lower":
					lower = []byte(arg[1])
				case "upper":
					upper = []byte(arg[1])
				default:
					return fmt.Sprintf("set-bounds: unknown arg: %s", arg)
				}
			}
			iter.SetBounds(lower, upper)
			continue
		case "stats":
			if o.stats != nil {
				// The timing is non-deterministic, so set to 0.
				o.stats.BlockReadDuration = 0
				fmt.Fprintf(&b, "%+v\n", *o.stats)
			}
			continue
		case "reset-stats":
			if o.stats != nil {
				*o.stats = base.InternalIteratorStats{}
			}
			continue
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		if key != nil {
			if o.verboseKey {
				fmt.Fprintf(&b, "%s:%s\n", key, value)
			} else {
				fmt.Fprintf(&b, "%s:%s\n", key.UserKey, value)
			}
		} else if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "err=%v\n", err)
		} else {
			fmt.Fprintf(&b, ".\n")
		}
	}
	return b.String()
}

// RunKeyspanIterCmd evaluates a datadriven command controlling an internal
// keyspan.FragmentIterator, returning a string with the results of the iterator
// operations.
func RunKeyspanIterCmd(
	t *testing.T, td *datadriven.TestData, iter keyspan.FragmentIterator,
) string {
	var buf bytes.Buffer
	lines := strings.Split(strings.TrimSpace(td.Input), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		i := strings.IndexByte(line, '#')
		iterCmd := line
		if i > 0 {
			iterCmd = string(line[:i])
		}
		RunKeyspanIterOp(&buf, iter, iterCmd)
		fmt.Fprintln(&buf)
	}
	return strings.TrimSpace(buf.String())
}

var iterDelim = map[rune]bool{',': true, ' ': true, '(': true, ')': true, '"': true}

func RunKeyspanIterOp(w io.Writer, it keyspan.FragmentIterator, op string) {
	fields := strings.FieldsFunc(op, func(r rune) bool { return iterDelim[r] })
	var s *keyspan.Span
	switch strings.ToLower(fields[0]) {
	case "first":
		s = it.First()
	case "last":
		s = it.Last()
	case "seekge", "seek-ge":
		if len(fields) == 1 {
			panic(fmt.Sprintf("unable to parse iter op %q", op))
		}
		s = it.SeekGE([]byte(fields[1]))
	case "seeklt", "seek-lt":
		if len(fields) == 1 {
			panic(fmt.Sprintf("unable to parse iter op %q", op))
		}
		s = it.SeekLT([]byte(fields[1]))
	case "next":
		s = it.Next()
	case "prev":
		s = it.Prev()
	default:
		panic(fmt.Sprintf("unrecognized iter op %q", fields[0]))
	}
	if s == nil {
		fmt.Fprint(w, "<nil>")
		if err := it.Error(); err != nil {
			fmt.Fprintf(w, " err=<%s>", it.Error())
		}
		return
	}
	fmt.Fprint(w, s)
}
