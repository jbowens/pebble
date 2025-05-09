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

func Define[T any](fields ...Field[T]) Definition[T] {
	var verticalHeader strings.Builder
	var verticalHeaderSep strings.Builder
	defFields := make([]definitionField[T], len(fields))
	maxFieldWidth := 0
	for i := 0; i < len(fields); i++ {
		maxFieldWidth = max(maxFieldWidth, fields[i].width())
	}

	for i := 0; i < len(fields); i++ {
		w := fields[i].width()
		h := fields[i].header(Vertically, maxFieldWidth)
		if len(h) > w {
			panic(fmt.Sprintf("header %q is too long for column %d", h, i))
		}

		defFields[i] = definitionField[T]{
			f:   fields[i],
			off: verticalHeaderSep.Len(),
		}

		// Create the vertical header strings.
		if _, ok := fields[i].(divider[T]); ok {
			verticalHeaderSep.WriteString("-+-")
		} else {
			verticalHeaderSep.WriteString(strings.Repeat("-", w))
		}
		padding := w - len(h)
		verticalHeader.WriteString(fields[i].align().maybePadding(AlignRight, padding))
		verticalHeader.WriteString(h)
		verticalHeader.WriteString(fields[i].align().maybePadding(AlignLeft, padding))
	}
	return Definition[T]{
		CumulativeFieldWidth: verticalHeaderSep.Len(),
		MaxFieldWidth:        maxFieldWidth,
		fields:               defFields,
		verticalHeaderLine:   verticalHeader.String(),
		verticalHeaderSep:    verticalHeaderSep.String(),
	}
}

type Definition[T any] struct {
	CumulativeFieldWidth int
	MaxFieldWidth        int
	fields               []definitionField[T]
	verticalHeaderLine   string
	verticalHeaderSep    string
}

type definitionField[T any] struct {
	f   Field[T]
	off int
}

func RenderAll[T any](fn func(r T) whiteboard.Pos, rows []T) whiteboard.Pos {
	pos := whiteboard.Pos{}
	for _, r := range rows {
		pos = fn(r)
	}
	return pos
}

type RenderOptions struct {
	Orientation Orientation
}

func (d *Definition[T]) RenderFunc(
	start whiteboard.Pos, opts RenderOptions,
) func(tuple T) whiteboard.Pos {
	pos := start

	if opts.Orientation == Vertically {
		pos.Offset(0, 0).WriteString(d.verticalHeaderLine)
		pos.Offset(1, 0).WriteString(d.verticalHeaderSep)
		r := 0
		return func(tuple T) whiteboard.Pos {
			for c := range d.fields {
				d.fields[c].f.renderValue(RenderContext[T]{
					Definition:  d,
					Orientation: Vertically,
					TupleIndex:  r,
					Pos:         pos.Offset(2+r, d.fields[c].off),
				}, tuple)
			}
			r++
			return pos.Offset(2+r, 0)
		}
	}

	for i := 0; i < len(d.fields); i++ {
		pos.Offset(i, 0).WriteString(d.fields[i].f.header(Horizontally, d.MaxFieldWidth))
		if _, ok := d.fields[i].f.(divider[T]); ok {
			pos.Offset(i, d.MaxFieldWidth).WriteString("-+-")
		} else {
			pos.Offset(i, d.MaxFieldWidth).WriteString(" | ")
		}
	}
	tupleIndex := 0
	c := d.MaxFieldWidth + 3
	return func(tuple T) whiteboard.Pos {
		for r := 0; r < len(d.fields); r++ {
			fieldOff := pos.Offset(r, c)
			d.fields[r].f.renderValue(RenderContext[T]{
				Definition:  d,
				Orientation: Horizontally,
				TupleIndex:  tupleIndex,
				Pos:         fieldOff,
			}, tuple)
		}
		tupleIndex++
		c += d.MaxFieldWidth
		return pos.Offset(len(d.fields), c)
	}
}

type RenderContext[T any] struct {
	Definition  *Definition[T]
	Orientation Orientation
	TupleIndex  int
	Pos         whiteboard.Pos
}

func (c *RenderContext[T]) PaddedPos(width int) whiteboard.Pos {
	if c.Orientation == Vertically {
		return c.Pos
	}
	// Horizontally, we need to pad the width to the max field width.
	return c.Pos.Offset(0, c.Definition.MaxFieldWidth-width)
}

func Div[T any]() Field[T] {
	return divider[T]{}
}

type divider[T any] struct{}

func (d divider[T]) header(o Orientation, maxWidth int) string {
	if o == Horizontally {
		return strings.Repeat("-", maxWidth)
	}
	return " | "
}
func (d divider[T]) width() int   { return 3 }
func (d divider[T]) align() Align { return AlignLeft }
func (d divider[T]) renderValue(ctx RenderContext[T], tuple T) {
	if ctx.Orientation == Horizontally {
		ctx.Pos.RepeatByte(ctx.Definition.MaxFieldWidth, '-')
	} else {
		ctx.Pos.WriteString(" | ")
	}
}

func Literal[T any](s string) Field[T] {
	return literal[T](s)
}

type literal[T any] string

func (l literal[T]) header(o Orientation, maxWidth int) string { return " " }
func (l literal[T]) width() int                                { return len(l) }
func (l literal[T]) align() Align                              { return AlignLeft }
func (l literal[T]) renderValue(ctx RenderContext[T], tuple T) {
	ctx.PaddedPos(len(l)).WriteString(string(l))
}

type Field[T any] interface {
	header(o Orientation, maxWidth int) string
	width() int
	align() Align
	renderValue(ctx RenderContext[T], tuple T)
}

const (
	AlignLeft Align = iota
	AlignRight
)

type Align uint8

func (a Align) maybePadding(ifAlign Align, width int) string {
	if a == ifAlign {
		return strings.Repeat(" ", width)
	}
	return ""
}

const (
	Vertically Orientation = iota
	Horizontally
)

type Orientation uint8

func String[T any](header string, width int, align Align, fn func(r T) string) Field[T] {
	spec := widthStr(width, align) + "s"
	return makeFuncField(header, width, align, func(ctx RenderContext[T], r T) {
		ctx.PaddedPos(width).Printf(spec, fn(r))
	})
}

func Int[T any](header string, width int, align Align, fn func(r T) int) Field[T] {
	spec := widthStr(width, align) + "d"
	return makeFuncField(header, width, align, func(ctx RenderContext[T], tuple T) {
		ctx.PaddedPos(width).Printf(spec, fn(tuple))
	})
}

func AutoIncrement[T any](header string, width int, align Align) Field[T] {
	spec := widthStr(width, align) + "d"
	return makeFuncField(header, width, align, func(ctx RenderContext[T], tuple T) {
		ctx.PaddedPos(width).Printf(spec, ctx.TupleIndex)
	})
}

func CountInt64[T any](header string, width int, align Align, fn func(r T) int64) Field[T] {
	spec := widthStr(width, align) + "s"
	return makeFuncField(header, width, align, func(ctx RenderContext[T], tuple T) {
		ctx.PaddedPos(width).Printf(spec, humanize.Count.Int64(fn(tuple)))
	})
}

func Count[T any](header string, width int, align Align, fn func(r T) uint64) Field[T] {
	spec := widthStr(width, align) + "s"
	return makeFuncField(header, width, align, func(ctx RenderContext[T], tuple T) {
		ctx.PaddedPos(width).Printf(spec, humanize.Count.Uint64(fn(tuple)))
	})
}

func BytesInt64[T any](header string, width int, align Align, fn func(r T) int64) Field[T] {
	spec := widthStr(width, align) + "s"
	return makeFuncField(header, width, align, func(ctx RenderContext[T], tuple T) {
		ctx.PaddedPos(width).Printf(spec, humanize.Bytes.Int64(fn(tuple)))
	})
}

func Bytes[T any](header string, width int, align Align, fn func(r T) uint64) Field[T] {
	spec := widthStr(width, align) + "s"
	return makeFuncField(header, width, align, func(ctx RenderContext[T], tuple T) {
		ctx.PaddedPos(width).Printf(spec, humanize.Bytes.Uint64(fn(tuple)))
	})
}

func Float[T any](header string, width int, align Align, fn func(r T) float64) Field[T] {
	spec := widthStr(width, align) + "s"
	return makeFuncField(header, width, align, func(ctx RenderContext[T], tuple T) {
		ctx.PaddedPos(width).Printf(spec, humanizeFloat(fn(tuple), width))
	})
}

func makeFuncField[T any](
	header string, width int, align Align, fn func(ctx RenderContext[T], tuple T),
) Field[T] {
	return &funcField[T]{
		headerValue: header,
		widthValue:  width,
		alignValue:  align,
		fn:          fn,
	}
}

type funcField[T any] struct {
	headerValue string
	widthValue  int
	alignValue  Align
	fn          func(ctx RenderContext[T], tuple T)
}

func (c *funcField[T]) header(o Orientation, maxWidth int) string { return c.headerValue }
func (c *funcField[T]) width() int                                { return c.widthValue }
func (c *funcField[T]) align() Align                              { return c.alignValue }
func (c *funcField[T]) renderValue(ctx RenderContext[T], tuple T) {
	c.fn(ctx, tuple)
}

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
