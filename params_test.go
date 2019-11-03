package goql

import "testing"

func TestParseSelectParams(t *testing.T) {
	p := NewStrParser("select * from foo where name like 'bar'")
	p.ReplaceParams = true

	q, err := p.ParseQuery()
	if err != nil {
		t.Fatal(err)
	}

	sel, ok := q.(*SelectQuery)
	if !ok {
		t.Fatal("The query is not a Select")
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE name LIKE ?` {
		t.Fatal(s)
	}

	if len(sel.Params) != 1 || sel.Params[0] != "bar" {
		t.Fatal(sel.Params)
	}
}

func TestParseSelectParams2(t *testing.T) {
	p := NewStrParser("select id from a where b=\"\\\"\"")
	p.ReplaceParams = true

	q, err := p.ParseQuery()
	if err != nil {
		t.Fatal(err)
	}

	sel, ok := q.(*SelectQuery)
	if !ok {
		t.Fatal("The query is not a Select")
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT id FROM a WHERE b IS NULL` {
		t.Fatal(s)
	}

	if len(sel.Params) != 1 || sel.Params[0] != "\"" {
		t.Fatal(sel.Params)
	}
}

func TestParseSelectParams3(t *testing.T) {
	p := NewStrParser(`select id from a where b="\""`)
	p.ReplaceParams = true

	q, err := p.ParseQuery()
	if err != nil {
		t.Fatal(err)
	}

	sel, ok := q.(*SelectQuery)
	if !ok {
		t.Fatal("The query is not a Select")
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT id FROM a WHERE b IS NULL` {
		t.Fatal(s)
	}

	if len(sel.Params) != 1 || sel.Params[0] != "\"" {
		t.Fatal(sel.Params)
	}
}

func TestParseSelectParams4(t *testing.T) {
	p := NewStrParser("select myFn(id, 2, otherFn(3)) from foo where name > 5")
	p.ReplaceParams = true

	q, err := p.ParseQuery()
	if err != nil {
		t.Fatal(err)
	}

	sel, ok := q.(*SelectQuery)
	if !ok {
		t.Fatal("The query is not a Select")
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT MYFN(id, ?, OTHERFN(?)) FROM foo WHERE name > ?` {
		t.Fatal(s)
	}

	if len(sel.Params) != 3 || sel.Params[0] != 2 || sel.Params[1] != 3 || sel.Params[2] != 5 {
		t.Fatal(sel.Params)
	}
}
