package goql

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

type lexer struct {
	Pos    Position
	reader *bufio.Reader
	Tokens []*Token
}

func newLexer(reader io.Reader) *lexer {
	return &lexer{
		Pos:    Position{0, 0, 0},
		reader: bufio.NewReaderSize(reader, 4096),
	}
}

func (l *lexer) error(tok string, msg string) *Error {
	return &Error{l.Pos, msg, tok}
}

const EOF_LINE int = -1

func (l *lexer) run() error {

	for {
		token := &Token{}

		l.skipWhiteSpace()
		c := l.next()
		if c == byte(EOF) {
			return nil
		}

		var buf bytes.Buffer

		switch {
		case isIdent(c, 0):
			token.Type = IDENT
			err := l.readIdent(c, &buf)
			token.Str = buf.String()
			if err != nil {
				return err
			}

			if typ, ok := reservedWords[strings.ToUpper(token.Str)]; ok {
				token.Type = typ
			}

		case isDecimal(c):
			if err := l.readNumber(c, &buf, token); err != nil {
				return err
			}

		default:
			switch c {
			case '"', '\'':
				token.Type = STRING
				err := l.readString(c, &buf)
				token.Str = buf.String()
				if err != nil {
					return err
				}
			case '`':
				token.Type = IDENT
				err := l.readIdent(c, &buf)
				token.Str = buf.String()
				if err != nil {
					return err
				}
				// readIdent won't read the closing quote
				if l.next() != '`' {
					return l.error(token.Str, "Unclosed back quote")
				}
			case '+':
				token.Type = ADD
				token.Str = string(c)
			case '-':
				if l.peek() == '-' {
					token.Type = COMMENT
					err := l.readComment(c, &buf)
					token.Str = buf.String()
					if err != nil {
						return err
					}
				} else {
					token.Type = SUB
					token.Str = string(c)
				}
			case '*':
				token.Type = MUL
				token.Str = string(c)
			case '/':
				token.Type = DIV
				token.Str = string(c)
			case '&':
				token.Type = ANB
				token.Str = string(c)
			case '=':
				token.Type = EQL
				token.Str = string(c)
			case '<':
				if l.peek() == '=' {
					token.Type = LEQ
					token.Str = "<="
					l.next()
				} else {
					token.Type = LSS
					token.Str = string(c)
				}
			case '>':
				switch l.peek() {
				case '=':
					token.Type = GEQ
					token.Str = ">="
					l.next()
				case '>':
					token.Type = LSF
					token.Str = ">>"
					l.next()
				default:
					token.Type = GTR
					token.Str = string(c)
				}
			case '!':
				if l.peek() == '=' {
					token.Type = NEQ
					token.Str = "!="
					l.next()
				} else {
					token.Type = NT
					token.Str = string(c)
				}
			case '%':
				token.Type = MOD
				token.Str = string(c)
			case '(':
				token.Type = LPAREN
				token.Str = string(c)
			case ')':
				token.Type = RPAREN
				token.Str = string(c)
			case ',':
				token.Type = COMMA
				token.Str = string(c)
			case '.':
				token.Type = PERIOD
				token.Str = string(c)
			case ':':
				token.Type = COLON
				token.Str = string(c)
			case ';':
				token.Type = SEMICOLON
				token.Str = string(c)
			case '?':
				token.Type = QUESTION
				token.Str = string(c)
			}
		}

		l.addToken(token)
	}
}

func (l *lexer) readNumber(c byte, buf *bytes.Buffer, token *Token) error {
	token.Type = INT
	err := l.readDecimal(c, buf)
	token.Str = buf.String()
	if err != nil {
		return err
	}
	c = l.peek()
	if c == '.' {
		buf.WriteByte(c)
		c = l.next()
		c = l.next()
		if !isDecimal(c) {
			return l.error(buf.String(), "Invalid number")
		}
		token.Type = FLOAT
		err = l.readDecimal(c, buf)
		token.Str = buf.String()
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *lexer) addToken(t *Token) {
	t.Pos = l.Pos
	t.Pos.Length = len(t.Str)
	l.Tokens = append(l.Tokens, t)
}

func (l *lexer) readString(quote byte, b *bytes.Buffer) error {
	c := l.next()
	for c != quote {
		// Allo multiline strings
		if c == byte(EOF) {
			return l.error(b.String(), "unterminated string")
		}

		// advance escape codes.
		if c == '\\' {
			c = l.next()
			switch c {
			case 'b':
				b.WriteByte('\b')
			case 't':
				b.WriteByte('\t')
			case 'n':
				b.WriteByte('\n')
			case 'f':
				b.WriteByte('\f')
			case 'r':
				b.WriteByte('\r')
			case '"':
				b.WriteByte('"')
			case '\'':
				b.WriteByte('\'')
			case '\\':
				b.WriteByte('\\')
			default:
				return l.error(b.String(), "Invalid escape sequence")
			}
			c = l.next()
			continue
		}

		b.WriteByte(c)
		c = l.next()
	}
	return nil
}

func (l *lexer) readComment(c byte, b *bytes.Buffer) error {
	b.WriteByte(c)
loop:
	for {
		switch l.peek() {
		case '\n', byte(EOF):
			break loop
		default:
			b.WriteByte(l.next())
		}
	}
	return nil
}

func (l *lexer) readIdent(c byte, b *bytes.Buffer) error {
	b.WriteByte(c)
	for isIdent(l.peek(), 1) {
		b.WriteByte(l.next())
	}
	return nil
}

func (l *lexer) readDecimal(c byte, b *bytes.Buffer) error {
	b.WriteByte(c)
	for isDecimal(l.peek()) {
		b.WriteByte(l.next())
	}
	return nil
}

func (l *lexer) readNext() byte {
	ch, err := l.reader.ReadByte()
	if err == io.EOF {
		return byte(EOF)
	}
	return ch
}

func (l *lexer) peek() byte {
	ch := l.readNext()
	if ch != byte(EOF) {
		l.reader.UnreadByte()
	}
	return ch
}

func (l *lexer) next() byte {
	ch := l.readNext()
	switch ch {
	case '\n', '\r':
		l.newline(ch)
		ch = '\n'
	case byte(EOF):
		l.Pos.Line = EOF_LINE
		l.Pos.Column = 0
	default:
		l.Pos.Column++
	}
	return ch
}

func (l *lexer) newline(ch byte) {
	l.Pos.Line += 1
	l.Pos.Column = 0
	next := l.peek()
	if ch == '\n' && next == '\r' || ch == '\r' && next == '\n' {
		l.reader.ReadByte()
	}
}

func (l *lexer) skipWhiteSpace() {
loop:
	if isWhitespace(l.peek()) {
		l.next()
		goto loop
	}
}

//const whitespace = 1<<'\t' | 1<<'\r' | 1<<' ' | 1<<'\n'

func isWhitespace(ch byte) bool {
	switch ch {
	case '\t', '\r', ' ', '\n':
		return true
	}
	return false
}

func isDecimal(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isIdent(ch byte, pos int) bool {
	return ch == '_' ||
		'A' <= ch && ch <= 'Z' ||
		'a' <= ch && ch <= 'z' ||
		isDecimal(ch) && pos > 0
}
