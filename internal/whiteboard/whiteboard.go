// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package whiteboard

import (
	"bytes"
	"fmt"
)

// ASCIIBoard is a simple ASCII-based board for rendering ASCII text diagrams.
type ASCIIBoard struct {
	buf   []byte
	width int
}

// Make returns a new ASCIIBoard with the given initial width and height.
func Make(w, h int) ASCIIBoard {
	buf := make([]byte, 0, w*h)
	return ASCIIBoard{buf: buf, width: w}
}

// At returns a position at the given coordinates.
func (b *ASCIIBoard) At(r, c int) Pos {
	if r >= b.lines() {
		b.buf = append(b.buf, bytes.Repeat([]byte{' '}, (r-b.lines()+1)*b.width)...)
	}
	return Pos{b: b, r: r, c: c}
}

// NewLine appends a new line to the board and returns a position at the
// beginning of the line.
func (b *ASCIIBoard) NewLine() Pos {
	return b.At(b.lines(), 0)
}

// String returns the ASCIIBoard as a string.
func (b *ASCIIBoard) String() string {
	return b.Render("")
}

// Render returns the ASCIIBoard as a string, with every line prefixed by
// indent.
func (b *ASCIIBoard) Render(indent string) string {
	var buf bytes.Buffer
	for r := 0; r < b.lines(); r++ {
		if r > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(indent)
		buf.Write(bytes.TrimRight(b.row(r), " "))
	}
	return buf.String()
}

// Reset resets the board to the given width and clears the contents.
func (b *ASCIIBoard) Reset(w int) {
	b.buf = b.buf[:0]
	b.width = w
}

func (b *ASCIIBoard) write(r, c int, s string) {
	if c+len(s) > b.width {
		b.growWidth(c + len(s))
	}
	row := b.row(r)
	for i := 0; i < len(s); i++ {
		row[c+i] = s[i]
	}
}

func (b *ASCIIBoard) repeat(r, c int, n int, ch byte) {
	if c+n > b.width {
		b.growWidth(c + n)
	}
	row := b.row(r)
	for i := 0; i < n; i++ {
		row[c+i] = ch
	}
}

func (b *ASCIIBoard) growWidth(w int) {
	buf := bytes.Repeat([]byte{' '}, w*b.lines())
	for i := 0; i < b.lines(); i++ {
		copy(buf[i*w:(i+1)*w], b.buf[i*b.width:(i+1)*b.width])
	}
	b.buf = buf
	b.width = w
}

func (b *ASCIIBoard) lines() int {
	return len(b.buf) / b.width
}

func (b *ASCIIBoard) row(r int) []byte {
	if sz := (r + 1) * b.width; sz > len(b.buf) {
		b.buf = append(b.buf, bytes.Repeat([]byte{' '}, sz-len(b.buf))...)
	}
	return b.buf[r*b.width : (r+1)*b.width]
}

// Pos is a position on an ASCIIBoard.
type Pos struct {
	b    *ASCIIBoard
	r, c int
}

// Offset returns a new position with the given offset from the current position.
func (p Pos) Offset(dr, dc int) Pos {
	return Pos{b: p.b, r: p.r + dr, c: p.c + dc}
}

// Row returns the row of the current position.
func (p Pos) Row() int {
	return p.r
}

// Column returns the column of the current position.
func (p Pos) Column() int {
	return p.c
}

// Printf writes the formatted string to pos, returning a position within the
// same row after the written string.
func (p Pos) Printf(format string, args ...interface{}) Pos {
	return p.WriteString(fmt.Sprintf(format, args...))
}

// WriteString writes the provided string to pos, returning a position within
// the same row after the written string.
func (p Pos) WriteString(s string) Pos {
	p.b.write(p.r, p.c, s)
	n := len(s)
	return p.Offset(0, n)
}

// RepeatByte writes the given byte n times to the current position, returning
// a position at the end of the written bytes.
func (p Pos) RepeatByte(n int, b byte) Pos {
	p.b.repeat(p.r, p.c, n, b)
	return p.Offset(0, n)
}

// NewlineReturn returns a position at the next line, with the column set to 0.
// NewlineReturn returns a position at the next line, with the column set to 0.
func (p Pos) NewlineReturn() Pos {
	return Pos{b: p.b, r: p.r + 1, c: 0}
}
