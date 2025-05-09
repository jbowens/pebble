// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package table

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/whiteboard"
)

func Define[R any](cols ...Column[R]) Definition[R] {
	var header strings.Builder
	var headerSep strings.Builder
	defCols := make([]definitionColumn[R], len(cols))
	for i := 0; i < len(cols); i++ {
		w := cols[i].width()
		h := cols[i].header()
		if len(h) > w {
			panic(fmt.Sprintf("header %q is too long for column %d", h, i))
		}

		defCols[i] = definitionColumn[R]{
			col:       cols[i],
			colOffset: headerSep.Len(),
		}
		if _, ok := cols[i].(divider[R]); ok {
			headerSep.WriteString("-+-")
		} else {
			headerSep.WriteString(strings.Repeat("-", w))
		}

		// Create the header, padding as necessary.
		padding := w - len(h)
		pad := func() {
			for j := 0; j < padding; j++ {
				header.WriteByte(' ')
			}
		}
		if cols[i].align() == AlignRight {
			pad()
			header.WriteString(h)
		} else {
			header.WriteString(h)
			pad()
		}
	}
	return Definition[R]{
		headerLine: header.String(),
		headerSep:  headerSep.String(),
		cols:       defCols,
		w:          headerSep.Len(),
	}
}

type Definition[R any] struct {
	headerLine string
	headerSep  string
	cols       []definitionColumn[R]
	w          int
}

type definitionColumn[R any] struct {
	col       Column[R]
	colOffset int
}

func (d *Definition[R]) Width() int {
	return d.w
}

func (d *Definition[R]) Render(start whiteboard.Pos, rows []R) whiteboard.Pos {
	pos := start
	pos.Offset(0, 0).WriteString(d.headerLine)
	pos.Offset(1, 0).WriteString(d.headerSep)
	for r := range rows {
		for c := range d.cols {
			d.cols[c].col.renderValue(rows[r], r, pos.Offset(2+r, d.cols[c].colOffset))
		}
	}
	return pos.Offset(2+len(rows), 0)
}

func (d *Definition[R]) RenderFunc(start whiteboard.Pos) func(r R) whiteboard.Pos {
	pos := start
	pos.Offset(0, 0).WriteString(d.headerLine)
	pos.Offset(1, 0).WriteString(d.headerSep)
	r := 0
	return func(row R) whiteboard.Pos {
		for c := range d.cols {
			d.cols[c].col.renderValue(row, r, pos.Offset(2+r, d.cols[c].colOffset))
		}
		r++
		return pos.Offset(2+r, 0)
	}
}

func Div[R any]() Column[R] {
	return divider[R]{}
}

type divider[R any] struct{}

func (d divider[R]) header() string                                { return " | " }
func (d divider[R]) width() int                                    { return 3 }
func (d divider[R]) align() Align                                  { return AlignLeft }
func (d divider[R]) renderValue(r R, rowIdx int, p whiteboard.Pos) { p.WriteString(" | ") }

func Literal[R any](s string) Column[R] {
	return literal[R](s)
}

type literal[R any] string

func (l literal[R]) header() string                                { return " " }
func (l literal[R]) width() int                                    { return len(l) }
func (l literal[R]) align() Align                                  { return AlignLeft }
func (l literal[R]) renderValue(r R, rowIdx int, p whiteboard.Pos) { p.WriteString(string(l)) }

type Column[R any] interface {
	header() string
	width() int
	align() Align
	renderValue(r R, rowIdx int, p whiteboard.Pos)
}

type Align uint8

const (
	AlignLeft Align = iota
	AlignRight
)

func String[R any](header string, width int, align Align, fn func(r R) string) Column[R] {
	spec := widthStr(width, align) + "s"
	return makeFuncColumn(header, width, align, func(r R, rowIdx int, p whiteboard.Pos) {
		p.Printf(spec, fn(r))
	})
}

func Int[R any](header string, width int, align Align, fn func(r R) int) Column[R] {
	spec := widthStr(width, align) + "d"
	return makeFuncColumn(header, width, align, func(r R, rowIdx int, p whiteboard.Pos) {
		p.Printf(spec, fn(r))
	})
}

func AutoIncrement[R any](header string, width int, align Align) Column[R] {
	spec := widthStr(width, align) + "d"
	return makeFuncColumn(header, width, align, func(r R, rowIdx int, p whiteboard.Pos) {
		p.Printf(spec, rowIdx)
	})
}

func CountInt64[R any](header string, width int, align Align, fn func(r R) int64) Column[R] {
	spec := widthStr(width, align) + "s"
	return makeFuncColumn(header, width, align, func(r R, rowIdx int, p whiteboard.Pos) {
		p.Printf(spec, humanize.Count.Int64(fn(r)))
	})
}

func Count[R any](header string, width int, align Align, fn func(r R) uint64) Column[R] {
	spec := widthStr(width, align) + "s"
	return makeFuncColumn(header, width, align, func(r R, rowIdx int, p whiteboard.Pos) {
		p.Printf(spec, humanize.Count.Uint64(fn(r)))
	})
}

func BytesInt64[R any](header string, width int, align Align, fn func(r R) int64) Column[R] {
	spec := widthStr(width, align) + "s"
	return makeFuncColumn(header, width, align, func(r R, rowIdx int, p whiteboard.Pos) {
		p.Printf(spec, humanize.Bytes.Int64(fn(r)))
	})
}

func Bytes[R any](header string, width int, align Align, fn func(r R) uint64) Column[R] {
	spec := widthStr(width, align) + "s"
	return makeFuncColumn(header, width, align, func(r R, rowIdx int, p whiteboard.Pos) {
		p.Printf(spec, humanize.Bytes.Uint64(fn(r)))
	})
}

func Float[R any](header string, width int, align Align, fn func(r R) float64) Column[R] {
	spec := widthStr(width, align) + "s"
	return makeFuncColumn(header, width, align, func(r R, rowIdx int, p whiteboard.Pos) {
		p.Printf(spec, humanizeFloat(fn(r), width))
	})
}

func makeFuncColumn[R any](
	header string, width int, align Align, fn func(r R, rowIdx int, p whiteboard.Pos),
) Column[R] {
	return &funcColumn[R]{
		headerValue: header,
		widthValue:  width,
		alignValue:  align,
		fn:          fn,
	}
}

type funcColumn[R any] struct {
	headerValue string
	widthValue  int
	alignValue  Align
	fn          func(r R, rowIdx int, p whiteboard.Pos)
}

func (c *funcColumn[R]) header() string                                { return c.headerValue }
func (c *funcColumn[R]) width() int                                    { return c.widthValue }
func (c *funcColumn[R]) align() Align                                  { return c.alignValue }
func (c *funcColumn[R]) renderValue(r R, rowIdx int, p whiteboard.Pos) { c.fn(r, rowIdx, p) }

func widthStr(width int, align Align) string {
	if align == AlignLeft {
		return "%-" + strconv.Itoa(width)
	}
	return "%" + strconv.Itoa(width)
}

// humanizeFloat formats a float64 value as a string. It shows up to two
// decimals, depending on the target length. NaN is shown as "-".
func humanizeFloat(v float64, targetLength int) string {
	if math.IsNaN(v) {
		return "-"
	}
	// We treat 0 specially. Values near zero will show up as 0.00.
	if v == 0 {
		return "0"
	}
	res := fmt.Sprintf("%.2f", v)
	if len(res) <= targetLength {
		return res
	}
	if len(res) == targetLength+1 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.0f", v)
}
