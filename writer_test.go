package goql

import (
	"strings"
	"testing"
)

func TestGroupConcat(t *testing.T) {
	q, err := ParseQuery("select group_concat(distinct v order by v asc separator ';') from t")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT GROUP_CONCAT(DISTINCT v ORDER BY v ASC SEPARATOR ';') FROM t` {
		t.Fatal(s)
	}
}

func TestSubquery(t *testing.T) {
	q, err := ParseQuery("select x.foo from (select a, b from bar) as x")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT x.foo FROM (SELECT a, b FROM bar) AS x` {
		t.Fatal(s)
	}
}

func TestSubquery2(t *testing.T) {
	q, err := ParseQuery("select a from (select * from bar)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT a FROM (SELECT * FROM bar)` {
		t.Fatal(s)
	}
}

func TestConcatSqlite(t *testing.T) {
	q, err := ParseQuery("select concat_ws(foo, bar)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT CONCAT_WS(foo, bar)` {
		t.Fatal(s)
	}

	s, _, err = toSQL(false, q, nil, "", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT foo || bar` {
		t.Fatal(s)
	}
}

func TestParseFunction(t *testing.T) {
	q, err := ParseQuery("select foo(22) from bar group by month(xx)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT FOO(22) FROM bar GROUP BY MONTH(xx)` {
		t.Fatal(s)
	}
}

func TestNullEquality(t *testing.T) {
	q, err := ParseQuery("select * from foo where a != null")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE a IS NOT NULL` {
		t.Fatal(s)
	}
}

func TestNullEquality2(t *testing.T) {
	q, err := ParseQuery("select * from foo where a != ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE a IS NOT NULL` {
		t.Fatal(s)
	}
}

func TestNullEquality3(t *testing.T) {
	q, err := ParseQuery("select * from foo where a != ? and b = ? and c = ?")
	if err != nil {
		t.Fatal(err)
	}

	params := []interface{}{1, "", nil}

	s, _, err := toSQL(false, q, params, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE a != ? AND b = ? AND c IS NULL` {
		t.Fatal(s)
	}
}

func TestBitwiseOperator(t *testing.T) {
	q, err := ParseQuery("select * from foo where (b >> ?) & 1")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE (b >> ?) & 1` {
		t.Fatal(s)
	}
}

func TestBetween(t *testing.T) {
	q, err := ParseQuery("select * from foo where id between ? and ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE id BETWEEN ? AND ?` {
		t.Fatal(s)
	}
}

func TestBasicSelect(t *testing.T) {
	q, err := ParseQuery("select f.*, bar from foo f")
	if err != nil {
		t.Fatal(err)
	}

	params := []interface{}{1, "", nil}

	s, _, err := toSQL(false, q, params, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT f.*, bar FROM foo AS f` {
		t.Fatal(s)
	}
}

func TestReplaceEmptyIN(t *testing.T) {
	q, err := ParseQuery("select * from foo where id in ?")
	if err != nil {
		t.Fatal(err)
	}

	var v []interface{}

	s, _, err := toSQL(false, q, v, "foo", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo.foo WHERE 1=0` {
		t.Fatal(s)
	}
}

func TestReplaceIN(t *testing.T) {
	q, err := ParseQuery("select * from foo where id in ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, []interface{}{[]interface{}{1, 2, 3}}, "foo", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo.foo WHERE id IN (1, 2, 3)` {
		t.Fatal(s)
	}
}

func TestReplaceIN0(t *testing.T) {
	q, err := ParseQuery("select * from foo where id in ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, []interface{}{[]interface{}{"1", "2", "3"}}, "foo", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo.foo WHERE id IN (1, 2, 3)` {
		t.Fatal(s)
	}
}

func TestReplaceIN1(t *testing.T) {
	q, err := ParseQuery("select * from foo where id in ?")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = toSQL(false, q, []interface{}{[]interface{}{1, true, 3}}, "foo", "")
	if err == nil {
		t.Fatal("Expected error. Only ints are valid")
	}
}

func TestReplaceIN11(t *testing.T) {
	q, err := ParseQuery("select * from foo where id in ?")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = toSQL(false, q, []interface{}{[]interface{}{"www"}}, "foo", "")
	if err == nil {
		t.Fatal("Expected error. Only ints are valid")
	}
}

func TestReplaceIN2(t *testing.T) {
	q, err := ParseQuery("select * from foo where id > ? AND id in ?")
	if err != nil {
		t.Fatal(err)
	}

	params := []interface{}{
		10,
		[]interface{}{1, 2, 3},
	}

	s, _, err := toSQL(false, q, params, "foo", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo.foo WHERE id > ? AND id IN (1, 2, 3)` {
		t.Fatal(s)
	}
}

func TestReplaceIN3(t *testing.T) {
	q, err := ParseQuery("select * from foo where id in ? AND id > ?")
	if err != nil {
		t.Fatal(err)
	}

	params := []interface{}{
		[]interface{}{1, 2, 3},
		10,
	}

	s, _, err := toSQL(false, q, params, "foo", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo.foo WHERE id IN (1, 2, 3) AND id > ?` {
		t.Fatal(s)
	}
}

func TestReplaceIN4(t *testing.T) {
	q, err := ParseQuery("select * from foo where id < ? and id in ? AND id > ?")
	if err != nil {
		t.Fatal(err)
	}

	params := []interface{}{
		20,
		[]interface{}{1, 2, 3},
		10,
	}

	s, _, err := toSQL(false, q, params, "foo", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo.foo WHERE id < ? AND id IN (1, 2, 3) AND id > ?` {
		t.Fatal(s)
	}
}

func TestDbPrefix(t *testing.T) {
	q, err := ParseQuery("select * from cars")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "foo", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo.cars` {
		t.Fatal(s)
	}
}

func TestDbPrefix2(t *testing.T) {
	q, err := ParseQuery("select id from cars")
	if err != nil {
		t.Fatal(err)
	}

	w := NewWriter(q, nil, "foo", "sqlite3")
	w.EscapeIdents = true

	s, _, err := w.Write()
	if err != nil {
		t.Fatal(err)
	}

	if s != "SELECT `id` FROM `foo_cars`" {
		t.Fatal(s)
	}
}

func TestDbPrefix3(t *testing.T) {
	q, err := ParseQuery("select id from cars")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "foo", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT id FROM foo_cars` {
		t.Fatal(s)
	}
}

func TestParseSelectCount(t *testing.T) {
	q, err := ParseQuery("select count(*) from cars")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT COUNT(*) FROM cars` {
		t.Fatal(s)
	}
}

func TestParseSelectOR(t *testing.T) {
	q, err := ParseQuery("select * from foo where a LIKE ? or b LIKE ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE a LIKE ? OR b LIKE ?` {
		t.Fatal(s)
	}
}

func TestParseSelect(t *testing.T) {
	q, err := ParseQuery("select * from cars")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM cars` {
		t.Fatal(s)
	}
}

func TestParseSelect1(t *testing.T) {
	q, err := ParseQuery("select 1 as num,true, false, null, 'te\"st'")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 AS num, true, false, null, "te\"st"` {
		t.Fatal(s)
	}
}

func TestParseSelect2(t *testing.T) {
	q, err := ParseQuery("select (1+2)*-5")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT (1 + 2) * -5` {
		t.Fatal(s)
	}
}

func TestParseSelect3(t *testing.T) {
	q, err := ParseQuery("select 1 from (select 1)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM (SELECT 1)` {
		t.Fatal(s)
	}
}

func TestParseSelect4(t *testing.T) {
	q, err := ParseQuery("select id from c order by name, age desc limit 3,4")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT id FROM c ORDER BY name, age DESC LIMIT 3, 4` {
		t.Fatal(s)
	}
}

func TestParseSelect5(t *testing.T) {
	q, err := ParseQuery("select id from c limit ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT id FROM c LIMIT ?` {
		t.Fatal(s)
	}
}

func TestParseSelect6(t *testing.T) {
	q, err := ParseQuery("select id from c limit ?,?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT id FROM c LIMIT ?, ?` {
		t.Fatal(s)
	}
}

func TestParseJoin(t *testing.T) {
	q, err := ParseQuery("select id from a join b on a.id = b.id")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT id FROM a JOIN b ON a.id = b.id` {
		t.Fatal(s)
	}
}

func TestParseJoin2(t *testing.T) {
	q, err := ParseQuery("select id from a left join b on a.id = b.id")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT id FROM a LEFT JOIN b ON a.id = b.id` {
		t.Fatal(s)
	}
}

func TestParseJoin3(t *testing.T) {
	q, err := ParseQuery("select a.id, b.* from a left join b on a.id = b.id")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT a.id, b.* FROM a LEFT JOIN b ON a.id = b.id` {
		t.Fatal(s)
	}
}

func TestParseWhere(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where true and (id < 3)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo WHERE true AND (id < 3)` {
		t.Fatal(s)
	}
}

func TestParseWhereIs(t *testing.T) {
	q, err := ParseQuery("select 1 is null, 1 is not null")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 IS null, 1 IS NOT null` {
		t.Fatal(s)
	}
}

func TestParseLike(t *testing.T) {
	q, err := ParseQuery("select * from foo where name like ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE name LIKE ?` {
		t.Fatal(s)
	}
}

func TestParseLike2(t *testing.T) {
	q, err := ParseQuery("select * from foo where name not like ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE name NOT LIKE ?` {
		t.Fatal(s)
	}
}

func TestParseWhere2(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where (select id from x) > 1")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "z", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM z.foo WHERE (SELECT id FROM z.x) > 1` {
		t.Fatal(s)
	}
}

func TestParseSelectDbPreffix1(t *testing.T) {
	q, err := ParseQuery("select a from customers c, payments p")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "foo", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "SELECT a FROM foo.customers AS c, foo.payments AS p" {
		t.Fatal(s)
	}
}

func TestParseSelectDbPreffix2(t *testing.T) {
	q, err := ParseQuery("select a from xx.customers c")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = toSQL(false, q, nil, "foo", "")
	if err == nil {
		t.Fatal("Should fail because it has a database already set")
	}
}

func TestParseSelectDbPreffix3(t *testing.T) {
	q, err := ParseQuery("select (select id from foo) from bar")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "db", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "SELECT (SELECT id FROM db.foo) FROM db.bar" {
		t.Fatal(s)
	}
}

func TestParseGroupBy(t *testing.T) {
	q, err := ParseQuery("select 1 from foo group by a,b")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo GROUP BY a, b` {
		t.Fatal(s)
	}
}

func TestParseWhereIN(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where id in (1,2)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo WHERE id IN (1, 2)` {
		t.Fatal(s)
	}
}

func TestParseWhereIN1(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where id in ?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, []interface{}{9}, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo WHERE id IN (9)` {
		t.Fatal(s)
	}
}

func TestParseWhereIN2(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where id in ('aa', 'bb')")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo WHERE id IN ("aa", "bb")` {
		t.Fatal(s)
	}
}

func TestParseWhereIN3(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where id in (1+2)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo WHERE id IN (1 + 2)` {
		t.Fatal(s)
	}
}

func TestParseWhereIN4(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where id in (select 1)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo WHERE id IN (SELECT 1)` {
		t.Fatal(s)
	}
}

func TestParseWhereIN5(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where id in ((select id from foo))")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo WHERE id IN ((SELECT id FROM foo))` {
		t.Fatal(s)
	}
}

func TestParseWhereIN6(t *testing.T) {
	q, err := ParseQuery("select 1 from foo where id not in (2,3)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT 1 FROM foo WHERE id NOT IN (2, 3)` {
		t.Fatal(s)
	}
}

func TestParseSelectFunc(t *testing.T) {
	q, err := ParseQuery("select now()")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT NOW()` {
		t.Fatal(s)
	}
}

func TestParseSelectFunc2(t *testing.T) {
	q, err := ParseQuery("select * from foo where d >= now()")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo WHERE d >= NOW()` {
		t.Fatal(s)
	}
}

func TestForUpdate(t *testing.T) {
	q, err := ParseQuery("select * from foo for update")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `SELECT * FROM foo FOR UPDATE` {
		t.Fatal(s)
	}
}

func TestParseDelete(t *testing.T) {
	q, err := ParseQuery("delete from foo where x = 'foo' and r = 'bar' limit 3")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "z", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `DELETE FROM z.foo WHERE x = "foo" AND r = "bar" LIMIT 3` {
		t.Fatal(s)
	}
}

func TestParseUpdate1(t *testing.T) {
	q, err := ParseQuery("update foo set x=3")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "UPDATE foo SET x = 3" {
		t.Fatal(s)
	}
}

func TestParseUpdate2(t *testing.T) {
	q, err := ParseQuery("update foo set x = (3+2) where id >= 10 limit 2")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "UPDATE foo SET x = (3 + 2) WHERE id >= 10 LIMIT 2" {
		t.Fatal(s)
	}
}

func TestParseUpdate3(t *testing.T) {
	q, err := ParseQuery("update post set title = concat(title, '-Z')")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE post SET title = CONCAT(title, "-Z")` {
		t.Fatal(s)
	}
}

func TestParseUpdate4(t *testing.T) {
	q, err := ParseQuery("UPDATE Employee SET password=?,webPunch=?,status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE Employee SET password = ?, webPunch = ?, status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoin(t *testing.T) {
	q, err := ParseQuery("UPDATE a JOIN b ON a.id = b.ida SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE a JOIN b ON a.id = b.ida SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoinLeft(t *testing.T) {
	q, err := ParseQuery("UPDATE a left JOIN b ON a.id = b.ida SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE a LEFT JOIN b ON a.id = b.ida SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoinRight(t *testing.T) {
	q, err := ParseQuery("UPDATE a right JOIN b ON a.id = b.ida SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE a RIGHT JOIN b ON a.id = b.ida SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoinOuter(t *testing.T) {
	q, err := ParseQuery("UPDATE a outer JOIN b ON a.id = b.ida SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE a OUTER JOIN b ON a.id = b.ida SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoinInner(t *testing.T) {
	q, err := ParseQuery("UPDATE a INNER JOIN b SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE a INNER JOIN b SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoinCross(t *testing.T) {
	q, err := ParseQuery("UPDATE a CROSS JOIN b SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE a CROSS JOIN b SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoinDouble(t *testing.T) {
	q, err := ParseQuery("UPDATE a JOIN b ON a.id = b.ida JOIN c ON b.id = c.idb SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE a JOIN b ON a.id = b.ida JOIN c ON b.id = c.idb SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoinMixed(t *testing.T) {
	q, err := ParseQuery("UPDATE a RIGHT JOIN b ON a.id = b.ida OUTER JOIN c ON b.id = c.idb SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE a RIGHT JOIN b ON a.id = b.ida OUTER JOIN c ON b.id = c.idb SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateJoinSqlite(t *testing.T) {
	q, err := ParseQuery("UPDATE a RIGHT JOIN b ON a.id = b.ida SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "sqlite3")
	if err == nil {
		t.Fatalf("Expected failure in sqlite3: %v", s)
	}

	if !strings.Contains(err.Error(), "UPDATE JOIN not supported in sqlite3") {
		t.Fatalf("Unexpected error got %v", err)
	}
}

func TestParseUpdateAlias(t *testing.T) {
	q, err := ParseQuery("UPDATE aa a JOIN bb b ON a.id = b.ida SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE aa AS a JOIN bb AS b ON a.id = b.ida SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseUpdateAliasAs(t *testing.T) {
	q, err := ParseQuery("UPDATE aa AS a JOIN bb AS b ON a.id = b.ida SET status=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `UPDATE aa AS a JOIN bb AS b ON a.id = b.ida SET status = ? WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseDelete2(t *testing.T) {
	q, err := ParseQuery("DELETE FROM Employee WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `DELETE FROM Employee WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseDeleteJoin(t *testing.T) {
	q, err := ParseQuery("DELETE a, b FROM a JOIN b ON a.id = bd.id")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `DELETE a, b FROM a JOIN b ON a.id = bd.id` {
		t.Fatal(s)
	}
}

func TestParseDeleteJoinLeft(t *testing.T) {
	q, err := ParseQuery("DELETE a,b FROM a left JOIN b ON a.id = b.ida WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `DELETE a, b FROM a LEFT JOIN b ON a.id = b.ida WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseDeleteJoinRight(t *testing.T) {
	q, err := ParseQuery("DELETE a,b FROM a right JOIN b ON a.id = b.ida WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `DELETE a, b FROM a RIGHT JOIN b ON a.id = b.ida WHERE id IS NULL` {
		t.Fatal(s)
	}
}

func TestParseDeleteJoinDouble(t *testing.T) {
	q, err := ParseQuery("DELETE a,b,c FROM a JOIN b ON a.id = b.ida JOIN c ON b.id = c.idb WHERE id>5")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `DELETE a, b, c FROM a JOIN b ON a.id = b.ida JOIN c ON b.id = c.idb WHERE id > 5` {
		t.Fatal(s)
	}
}

func TestParseDeleteJoinMixed(t *testing.T) {
	q, err := ParseQuery("DELETE a,b,c FROM a LEFT JOIN b ON a.id = b.ida RIGHT JOIN c ON b.id = c.idb WHERE id>5")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != `DELETE a, b, c FROM a LEFT JOIN b ON a.id = b.ida RIGHT JOIN c ON b.id = c.idb WHERE id > 5` {
		t.Fatal(s)
	}
}

func TestParseDeleteJoinSqlite(t *testing.T) {
	q, err := ParseQuery("Delete a,b FROM a RIGHT JOIN b ON a.id = b.ida WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "sqlite3")
	if err == nil {
		t.Fatalf("Expected failure in sqlite3: %v", s)
	}
}

func TestParseInsert1(t *testing.T) {
	q, err := ParseQuery("insert into foo values (3, 4)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "INSERT INTO foo VALUES (3, 4)" {
		t.Fatal(s)
	}
}

func TestParseInsert2(t *testing.T) {
	q, err := ParseQuery("insert into foo (id, id2) values (3, 4)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "INSERT INTO foo (id, id2) VALUES (3, 4)" {
		t.Fatal(s)
	}
}

func TestParseInsert3(t *testing.T) {
	q, err := ParseQuery("insert into foo values (3, 4)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "x", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "INSERT INTO x.foo VALUES (3, 4)" {
		t.Fatal(s)
	}
}

func TestParseInsert4(t *testing.T) {
	q, err := ParseQuery("insert into foo values (?, ?)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "INSERT INTO foo VALUES (?, ?)" {
		t.Fatal(s)
	}
}

func TestParseInsert5(t *testing.T) {
	q, err := ParseQuery("insert into foo values (default)")
	if err != nil {
		t.Fatal(err)
	}

	s, _, err := toSQL(false, q, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if s != "INSERT INTO foo VALUES (default)" {
		t.Fatal(s)
	}
}

func TestParseIgnoreNamespaces(t *testing.T) {
	q, err := ParseQuery("select * from foo:bar:buzz")
	if err != nil {
		t.Fatal(err)
	}

	w := NewWriter(q, nil, "", "")
	w.IgnoreNamespaces = true
	w.EscapeIdents = false
	s, _, err := w.Write()
	if err != nil {
		t.Fatal(err)
	}

	if s != "SELECT * FROM foo:bar:buzz" {
		t.Fatal(s)
	}
}

func toSQL(format bool, q Query, params []interface{}, database, driver string) (string, []interface{}, error) {
	w := NewWriter(q, params, database, driver)
	w.EscapeIdents = false
	w.Format = format
	return w.Write()
}
