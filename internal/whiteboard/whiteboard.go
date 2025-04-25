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
func (b *ASCIIBoard) At(x, y int) Pos {
	if y >= b.lines() {
		b.buf = append(b.buf, bytes.Repeat([]byte{' '}, (y-b.lines()+1)*b.width)...)
	}
	return Pos{b: b, x: x, y: y}
}

// NewLine appends a new line to the board and returns a position at the
// beginning of the line.
func (b *ASCIIBoard) NewLine() Pos {
	return b.At(0, b.lines())
}

// String returns the ASCIIBoard as a string.
func (b *ASCIIBoard) String() string {
	return b.Render("")
}

// Render returns the ASCIIBoard as a string, with every line prefixed by
// indent.
func (b *ASCIIBoard) Render(indent string) string {
	var buf bytes.Buffer
	for i := 0; i < b.lines(); i++ {
		if i > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(indent)
		buf.Write(bytes.TrimRight(b.row(i), " "))
	}
	return buf.String()
}

func (b *ASCIIBoard) write(x, y int, s string) {
	if x+len(s) > b.width {
		b.growWidth(x + len(s))
	}
	r := b.row(y)
	for i := 0; i < len(s); i++ {
		r[x+i] = s[i]
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

func (b *ASCIIBoard) row(x int) []byte {
	if sz := (x + 1) * b.width; sz > len(b.buf) {
		b.buf = append(b.buf, bytes.Repeat([]byte{' '}, sz-len(b.buf))...)
	}
	return b.buf[x*b.width : (x+1)*b.width]
}

// Pos is a position on an ASCIIBoard.
type Pos struct {
	b    *ASCIIBoard
	x, y int
}

// Offset returns a new position with the given offset from the current position.
func (p Pos) Offset(dx, dy int) Pos {
	return Pos{b: p.b, x: p.x + dx, y: p.y + dy}
}

// Printf writes the formatted string to pos, returning a position within the
// same row after the written string.
func (p Pos) Printf(format string, args ...interface{}) Pos {
	return p.WriteString(fmt.Sprintf(format, args...))
}

// WriteString writes the provided string to pos, returning a position within
// the same row after the written string.
func (p Pos) WriteString(s string) Pos {
	p.b.write(p.x, p.y, s)
	n := len(s)
	return p.Offset(n, 0)
}
