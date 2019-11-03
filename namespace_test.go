package goql

import (
	"fmt"
	"strings"
	"testing"
)

func TestNamespace(t *testing.T) {
	query := `SELECT * FROM client`
	expected := `SELECT * FROM fiz_foo_client`
	shouldFail := false

	if err := testNamespace(query, expected, "", "fiz:foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace2(t *testing.T) {
	query := `SELECT * FROM client c JOIN sale s ON s.idClient = c.id`
	expected := `SELECT * FROM foo_client AS c JOIN foo_sale AS s ON s.idClient = c.id`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace3(t *testing.T) {
	query := `SELECT * FROM bar:client AS c JOIN sale AS s ON s.idClient = c.id`
	expected := `SELECT * FROM bar_client AS c JOIN foo_sale AS s ON s.idClient = c.id`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace4(t *testing.T) {
	query := `SELECT * FROM fiz:bar:client AS c JOIN sale AS s ON s.idClient = c.id`
	expected := `SELECT * FROM fiz_bar_client AS c JOIN buz_foo_sale AS s ON s.idClient = c.id`
	shouldFail := false

	if err := testNamespace(query, expected, "", "buz:foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace5(t *testing.T) {
	query := `UPDATE bar:client SET idClient = 2`
	expected := ``
	shouldFail := true

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace51(t *testing.T) {
	query := `DELETE FROM bar:client`
	expected := ``
	shouldFail := true

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace6(t *testing.T) {
	query := `INSERT INTO bar:client VALUES (?)`
	expected := ``
	shouldFail := true

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace7(t *testing.T) {
	query := `UPDATE client SET idClient = 2`
	expected := `UPDATE foo_client SET idClient = 2`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace8(t *testing.T) {
	query := `INSERT INTO client VALUES (?)`
	expected := `INSERT INTO foo_client VALUES (?)`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace9(t *testing.T) {
	query := `UPDATE client SET idClient = 2`
	expected := `UPDATE foo_client SET idClient = 2`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace11(t *testing.T) {
	query := `CREATE TABLE client (name VARCHAR(30))`
	expected := `CREATE TABLE foo_client (name VARCHAR(30) NOT NULL COLLATE NOCASE)`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace12(t *testing.T) {
	query := `CREATE TABLE bar:client (name VARCHAR(30))`
	expected := ``
	shouldFail := true

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace13(t *testing.T) {
	query := `INSERT INTO client SELECT name FROM user`
	expected := `INSERT INTO foo_client SELECT name FROM foo_user`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace14(t *testing.T) {
	query := `INSERT INTO client SELECT name FROM bar:user`
	expected := `INSERT INTO foo_client SELECT name FROM bar_user`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace15(t *testing.T) {
	query := `INSERT INTO client VALUES (1, (SELECT name FROM bar:user))`
	expected := `INSERT INTO foo_client VALUES (1, (SELECT name FROM bar_user))`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace16(t *testing.T) {
	query := `INSERT INTO bar:client VALUES (1, (SELECT name FROM bar:user))`
	expected := ``
	shouldFail := true

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace17(t *testing.T) {
	query := `INSERT INTO bar:client SELECT name FROM bar:user`
	expected := ``
	shouldFail := true

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace20(t *testing.T) {
	query := `INSERT INTO foo:bar:client SELECT name FROM bar:user`
	expected := `INSERT INTO foo_bar_client SELECT name FROM bar_user`
	shouldFail := false

	if err := testNamespace(query, expected, "", "foo", true, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace201(t *testing.T) {
	query := `INSERT INTO fizz:foo:bar:client SELECT name FROM bar:user`
	expected := ``
	shouldFail := true

	if err := testNamespace(query, expected, "", "foo", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace21(t *testing.T) {
	query := `SELECT name FROM bar:user`
	expected := `SELECT name FROM bar_user`
	shouldFail := false

	if err := testNamespace(query, expected, "", "", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace211(t *testing.T) {
	query := `INSERT INTO foo:client SELECT name FROM bar:user`
	expected := `INSERT INTO foo_client SELECT name FROM bar_user`
	shouldFail := false

	if err := testNamespace(query, expected, "", "", true, shouldFail); err != nil {
		t.Fatal(err)
	}
}

func TestNamespace22(t *testing.T) {
	query := `INSERT INTO client SELECT name FROM user`
	expected := `INSERT INTO client SELECT name FROM user`
	shouldFail := false

	if err := testNamespace(query, expected, "", "", false, shouldFail); err != nil {
		t.Fatal(err)
	}
}

// add here tests trying to accept invalid queries, query other database
// if restricted or any other vulnerability.
// SQL injection prevention is not possible because they are valid queries.
func TestInvalidDatabase(t *testing.T) {
	queries := []string{
		"show databases",
		"show tables from db2",
		"show columns from db2.foo",
		"select count(*) from db2.cars",
		"select id, (select id2 from db2.cars) from cars",
		"select id, (select(select(select 1 from db2.cars))) from cars",
		"select id from cars c join db2.items j",
		`select id from cars a 
		join items b on a = (select id from db2.x)`,
		`select 1 from cars WHERE a in (select id from db2.x)`,
		`select id from cars a UNION select id from db2.x`,
	}

	for i, s := range queries {
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if _, _, err := toSQL(false, q, nil, "db1", ""); !isInvalidDB(err) {
			t.Fatalf("%d: Expected invalid database error: %v", i, err)
		}
	}
}

// Make sure to don't accept invalid queries
func TestInvalidQuery(t *testing.T) {
	queries := []string{
		"show columns from db2;foo",
		"show columns from db2%foo",
		"select * from db2%foo",
		"select n'm from foo",
		"show tables;select * from foo",
		"select 1;select * from foo",
		"select asdf= from foo",
		"select asdf?0 from foo",
		"select asd\\a from foo",
		"select asd//a from foo",
	}

	for i, q := range queries {
		if _, err := ParseQuery(q); err == nil {
			t.Fatalf("%d: Expected invalid query: %s", i, q)
		}
	}
}

func isInvalidDB(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "Invalid database ")
}

func testNamespace(query, expected, database, namespace string, writeAll bool, shouldFail bool) error {
	q, err := ParseQuery(query)
	if err != nil {
		return err
	}

	w := NewWriter(q, nil, database, "sqlite3")
	w.Namespace = namespace
	w.NamespaceWriteAll = writeAll
	w.EscapeIdents = false
	query, _, err = w.Write()

	if shouldFail {
		if err == nil {
			return fmt.Errorf("Expected to fail but didn't")
		}
		return nil
	}

	if err != nil {
		return err
	}

	if !strings.EqualFold(query, expected) {
		return fmt.Errorf("Expected\n\t%s\ngot\n\t%s", expected, query)
	}

	return nil
}
