package goql

import (
	"fmt"
	"strings"
	"testing"
)

func TestLex(t *testing.T) {
	data := []struct {
		s string
		t []Type
	}{
		{"-1", []Type{SUB, INT}},
		{"SELECT * FROM test", []Type{SELECT, MUL, FROM, IDENT}},
		{"SELECT `name` FROM test", []Type{SELECT, IDENT, FROM, IDENT}},
		{"WHERE 1", []Type{WHERE, INT}},
		{"-- foo bar", []Type{COMMENT}},
		{"8.99 -- foo foo", []Type{FLOAT, COMMENT}},
		{`SELECT 'asdf
				  asdfasdf' FROM test`, []Type{SELECT, STRING, FROM, IDENT}},
	}

	for i, d := range data {
		if err := test(d.s, d.t); err != nil {
			t.Fatalf("test [%d] %v", i, err)
		}
	}
}

func test(s string, types []Type) error {
	l := newLexer(strings.NewReader(s))

	if err := l.run(); err != nil {
		return err
	}

	if len(types) != len(l.Tokens) {
		return fmt.Errorf("found %d tokens, expected %d", len(l.Tokens), len(types))
	}

	for i, t := range types {
		lt := l.Tokens[i]
		if lt.Type != t {
			return fmt.Errorf("%d. found %v, expected %v", i, lt.Type, t)
		}
	}

	return nil
}
