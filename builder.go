package goql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

//// GetMainTable returns the main table of a query
//func GetMainTable(q *SelectQuery) *Table {
//	if len(q.From) == 0 {
//		return nil
//	}

//	f, ok := q.From[0].(*Table)
//	if ok {
//		return f
//	}

//	return nil
//}

//// GetSelectColumn searches for a select column with that name or alias.
//func GetSelectColumn(nameOrAlias string, q *SelectQuery) Expr {
//	for _, c := range q.Columns {
//		switch t := c.(type) {
//		case *SelectColumnExpr:
//			if strings.EqualFold(t.Alias, nameOrAlias) {
//				return t
//			}
//		case *ColumnNameExpr:
//			if strings.EqualFold(t.Name, nameOrAlias) {
//				return t
//			}
//			if t.Alias != "" && strings.EqualFold(t.Alias, nameOrAlias) {
//				return t
//			}
//		}
//	}

//	return nil
//}

// ExprColumns returns the column names in an expression.
func NameExprColumns(e Expr) []*ColumnNameExpr {
	var c []*ColumnNameExpr

	switch t := e.(type) {
	case *ColumnNameExpr:
		c = append(c, t)
	case *SelectQuery:
		c = append(c, selectColumnNames(t)...)
	case *BinaryExpr:
		c = append(c, NameExprColumns(t.Left)...)
		c = append(c, NameExprColumns(t.Right)...)
	case *CallExpr:
		for _, l := range t.Args {
			c = append(c, NameExprColumns(l)...)
		}
	case *InExpr:
		for _, l := range t.Values {
			c = append(c, NameExprColumns(l)...)
		}
	}

	return c
}

// selectColumnNames returns the columnNames in an expression
func selectColumnNames(s *SelectQuery) []*ColumnNameExpr {
	var c []*ColumnNameExpr

	for _, l := range s.Columns {
		c = append(c, NameExprColumns(l)...)
	}

	for _, f := range s.From {
		switch k := f.(type) {
		case *Table:
			for _, l := range k.Joins {
				c = append(c, NameExprColumns(l.On)...)
			}
		case *ParenExpr:
			c = append(c, NameExprColumns(k.X)...)
		}
	}

	if s.WherePart != nil {
		c = append(c, NameExprColumns(s.WherePart.Expr)...)
	}

	for _, l := range s.GroupByPart {
		c = append(c, NameExprColumns(l)...)
	}

	if s.HavingPart != nil {
		c = append(c, NameExprColumns(s.HavingPart.Expr)...)
	}

	for _, l := range s.OrderByPart {
		c = append(c, NameExprColumns(l.Expr)...)
	}
	for _, l := range s.UnionPart {
		c = append(c, selectColumnNames(l)...)
	}

	return c
}

func (q *SelectQuery) SetColumns(code string) error {
	q.Columns = nil
	return q.AddColumns(code)
}

func (q *SelectQuery) AddColumns(code string) error {
	p := NewStrParser(code)
	if err := p.lexer.run(); err != nil {
		return err
	}

	exps, err := p.parseSelectColumns()
	if err != nil {
		return err
	}

	q.Columns = append(q.Columns, exps...)
	return nil
}

func (q *SelectQuery) SetFrom(code string) error {
	p := NewStrParser(code)
	if err := p.lexer.run(); err != nil {
		return err
	}

	froms, err := p.parseFrom()
	if err != nil {
		return err
	}

	q.From = froms
	return nil
}

func (q *SelectQuery) Join(code string) error {
	p := NewStrParser(code)
	if err := p.lexer.run(); err != nil {
		return err
	}

	if len(q.From) == 0 {
		return fmt.Errorf("Can't add a join to this query (empty FROM)")
	}

	t, ok := q.From[0].(*Table)
	if !ok {
		return fmt.Errorf("Can't add a join to this query. From must be a table")
	}

	switch p.peek().Type {
	case LEFT, RIGHT, OUTER, JOIN:
	default:
		// if no join type is specified, set a default join
		k := &Token{Type: JOIN}
		p.lexer.Tokens = append([]*Token{k}, p.lexer.Tokens...)
	}

	js, err := p.parseJoins()
	if err != nil {
		return err
	}

	t.Joins = append(t.Joins, js...)
	return nil
}

func (q *SelectQuery) GroupBy(code string) error {
	if !strings.HasPrefix(strings.ToUpper(code), "GROUP BY ") {
		code = "GROUP BY " + code
	}

	p := NewStrParser(code)
	if err := p.lexer.run(); err != nil {
		return err
	}

	g, err := p.parseGroupBy()
	if err != nil {
		return err
	}

	q.GroupByPart = append(q.GroupByPart, g...)
	return nil
}

func (q *SelectQuery) Where(code string, params ...interface{}) error {
	return q.And(code, params...)
}

func (q *SelectQuery) And(code string, params ...interface{}) error {
	p := NewStrParser(code)

	if err := p.lexer.run(); err != nil {
		return err
	}

	// the filter can be empty but have order, having, etc...
	// TODO: handle group by, etc....
	isFilter := !strings.HasPrefix(strings.ToUpper(code), "ORDER BY")
	if isFilter {
		exp, err := p.parseBooleanExpr()
		if err != nil {
			return err
		}

		processParams(params)

		q.Params = append(q.Params, p.Params...)
		q.Params = append(q.Params, params...)

		if q.WherePart != nil {
			q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: AND}
			return nil
		}

		q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}
	}

	if err := parseAfterFilter(q, p); err != nil {
		return err
	}

	return nil
}

func parseAfterFilter(q *SelectQuery, p *Parser) error {
	o, err := p.parseOrderBy()
	if err == nil {
		q.OrderByPart = append(q.OrderByPart, o...)
	}

	g, err := p.parseGroupBy()
	if err == nil {
		q.GroupByPart = append(q.GroupByPart, g...)
	}

	h, ok, err := p.parseHaving()
	if err != nil {
		return err
	}

	if ok {
		if q.HavingPart == nil {
			q.HavingPart = h
		} else {
			q.HavingPart.Expr = &BinaryExpr{Left: q.HavingPart.Expr, Right: h.Expr, Operator: AND}
		}
	}

	return nil
}

func (q *SelectQuery) AndQuery(filter *SelectQuery) {
	if filter == nil {
		return
	}

	q.Params = append(q.Params, filter.Params...)

	exp := &ParenExpr{X: filter.WherePart.Expr}

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: AND}
		return
	}
	q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}
}

func (q *SelectQuery) Or(code string, params ...interface{}) error {
	p := NewStrParser(code)

	if err := p.lexer.run(); err != nil {
		return err
	}

	// the filter can be empty but have order, having, etc...
	// TODO: handle group by, etc....
	isFilter := !strings.HasPrefix(strings.ToUpper(code), "ORDER BY")
	if isFilter {
		e, err := p.parseBooleanTerm()
		if err != nil {
			return err
		}

		processParams(params)

		q.Params = append(q.Params, params...)

		if q.WherePart != nil {
			q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: e, Operator: OR}
			return nil
		}

		q.WherePart = &WherePart{Pos: e.Position(), Expr: e}
	}

	if err := parseAfterFilter(q, p); err != nil {
		return err
	}

	return nil
}

func (q *SelectQuery) OrQuery(filter *SelectQuery) {
	if filter == nil {
		return
	}

	q.Params = append(q.Params, filter.Params...)

	exp := filter.WherePart.Expr

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: OR}
		return
	}
	q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}
}

func (q *SelectQuery) Limit(rowCount int) {
	if rowCount == 0 {
		q.LimitPart = nil
		return
	}

	q.LimitPart = &Limit{
		RowCount: &ConstantExpr{Kind: INT, Value: strconv.Itoa(rowCount)},
	}
}

func (q *SelectQuery) LimitOffset(offset, rowCount int) {
	if rowCount == 0 {
		q.LimitPart = nil
		return
	}

	q.LimitPart = &Limit{
		Offset:   &ConstantExpr{Kind: INT, Value: strconv.Itoa(offset)},
		RowCount: &ConstantExpr{Kind: INT, Value: strconv.Itoa(rowCount)},
	}
}

func (q *SelectQuery) OrderBy(code string) error {
	r := strings.NewReader(code)
	p := &Parser{lexer: newLexer(r)}
	if err := p.lexer.run(); err != nil {
		return err
	}

	// insert the "ORDER BY" part
	k := []*Token{&Token{Type: ORDER}, &Token{Type: BY}}
	p.lexer.Tokens = append(k, p.lexer.Tokens...)

	o, err := p.parseOrderBy()
	if err != nil {
		return err
	}

	q.OrderByPart = append(q.OrderByPart, o...)
	return nil
}

func (q *SelectQuery) Having(code string, params ...interface{}) error {
	r := strings.NewReader(code)
	p := &Parser{lexer: newLexer(r)}
	if err := p.lexer.run(); err != nil {
		return err
	}

	h, err := p.parseHavingPart()
	if err != nil {
		return err
	}

	processParams(params)

	q.Params = append(q.Params, params...)

	if q.HavingPart == nil {
		q.HavingPart = h
	} else {
		q.HavingPart.Expr = &BinaryExpr{Left: q.HavingPart.Expr, Right: h.Expr, Operator: AND}
	}

	return nil
}

func (q *DeleteQuery) Where(code string, params ...interface{}) error {
	return q.And(code, params...)
}

func (q *DeleteQuery) And(code string, params ...interface{}) error {
	p := NewStrParser(code)

	if err := p.lexer.run(); err != nil {
		return err
	}

	exp, err := p.parseBooleanExpr()
	if err != nil {
		return err
	}

	processParams(params)

	q.Params = append(q.Params, p.Params...)
	q.Params = append(q.Params, params...)

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: AND}
		return nil
	}

	q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}

	return nil
}

func (q *DeleteQuery) AndQuery(filter *SelectQuery) {
	if filter == nil {
		return
	}

	q.Params = append(q.Params, filter.Params...)

	exp := &ParenExpr{X: filter.WherePart.Expr}

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: AND}
		return
	}
	q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}
}

func (q *DeleteQuery) Or(code string, params ...interface{}) error {
	p := NewStrParser(code)

	if err := p.lexer.run(); err != nil {
		return err
	}

	e, err := p.parseBooleanTerm()
	if err != nil {
		return err
	}

	processParams(params)

	q.Params = append(q.Params, params...)

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: e, Operator: OR}
		return nil
	}

	q.WherePart = &WherePart{Pos: e.Position(), Expr: e}

	return nil
}

func (q *DeleteQuery) OrQuery(filter *SelectQuery) {
	if filter == nil {
		return
	}

	q.Params = append(q.Params, filter.Params...)

	exp := filter.WherePart.Expr

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: OR}
		return
	}
	q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}
}

func (q *DeleteQuery) Limit(rowCount int) {
	if rowCount == 0 {
		q.LimitPart = nil
		return
	}

	q.LimitPart = &Limit{
		RowCount: &ConstantExpr{Kind: INT, Value: strconv.Itoa(rowCount)},
	}
}

func (q *DeleteQuery) LimitOffset(offset, rowCount int) {
	if rowCount == 0 {
		q.LimitPart = nil
		return
	}

	q.LimitPart = &Limit{
		Offset:   &ConstantExpr{Kind: INT, Value: strconv.Itoa(offset)},
		RowCount: &ConstantExpr{Kind: INT, Value: strconv.Itoa(rowCount)},
	}
}

func (q *DeleteQuery) Join(code string) error {
	p := NewStrParser(code)
	if err := p.lexer.run(); err != nil {
		return err
	}

	switch p.peek().Type {
	case LEFT, RIGHT, INNER, OUTER, CROSS, JOIN:
	default:
		// if no join type is specified, set a default join
		k := &Token{Type: JOIN}
		p.lexer.Tokens = append([]*Token{k}, p.lexer.Tokens...)
	}

	joins, err := p.parseJoins()
	if err != nil {
		return err
	}

	q.Table.Joins = append(q.Table.Joins, joins...)
	return nil
}

func (q *UpdateQuery) Where(code string, params ...interface{}) error {
	return q.And(code, params...)
}

func (q *UpdateQuery) And(code string, params ...interface{}) error {
	p := NewStrParser(code)

	if err := p.lexer.run(); err != nil {
		return err
	}

	exp, err := p.parseBooleanExpr()
	if err != nil {
		return err
	}

	processParams(params)

	q.Params = append(q.Params, p.Params...)
	q.Params = append(q.Params, params...)

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: AND}
		return nil
	}

	q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}

	return nil
}

func (q *UpdateQuery) AndQuery(filter *SelectQuery) {
	if filter == nil {
		return
	}

	q.Params = append(q.Params, filter.Params...)

	exp := &ParenExpr{X: filter.WherePart.Expr}

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: AND}
		return
	}
	q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}
}

func (q *UpdateQuery) Or(code string, params ...interface{}) error {
	p := NewStrParser(code)

	if err := p.lexer.run(); err != nil {
		return err
	}

	e, err := p.parseBooleanTerm()
	if err != nil {
		return err
	}

	processParams(params)

	q.Params = append(q.Params, params...)

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: e, Operator: OR}
		return nil
	}

	q.WherePart = &WherePart{Pos: e.Position(), Expr: e}

	return nil
}

func (q *UpdateQuery) OrQuery(filter *SelectQuery) {
	if filter == nil {
		return
	}

	q.Params = append(q.Params, filter.Params...)

	exp := filter.WherePart.Expr

	if q.WherePart != nil {
		q.WherePart.Expr = &BinaryExpr{Left: q.WherePart.Expr, Right: exp, Operator: OR}
		return
	}
	q.WherePart = &WherePart{Pos: exp.Position(), Expr: exp}
}

func (q *UpdateQuery) Limit(rowCount int) {
	if rowCount == 0 {
		q.LimitPart = nil
		return
	}

	q.LimitPart = &Limit{
		RowCount: &ConstantExpr{Kind: INT, Value: strconv.Itoa(rowCount)},
	}
}

func (q *UpdateQuery) LimitOffset(offset, rowCount int) {
	if rowCount == 0 {
		q.LimitPart = nil
		return
	}

	q.LimitPart = &Limit{
		Offset:   &ConstantExpr{Kind: INT, Value: strconv.Itoa(offset)},
		RowCount: &ConstantExpr{Kind: INT, Value: strconv.Itoa(rowCount)},
	}
}

func (q *UpdateQuery) SetColumns(code string) error {
	q.Columns = nil
	return q.AddColumns(code)
}

func (q *UpdateQuery) AddColumns(code string) error {
	p := NewStrParser(code)
	if err := p.lexer.run(); err != nil {
		return err
	}

	exps, err := p.parseColumnValues()
	if err != nil {
		return err
	}

	q.Columns = append(q.Columns, exps...)
	return nil
}

func (q *UpdateQuery) Join(code string) error {
	p := NewStrParser(code)
	if err := p.lexer.run(); err != nil {
		return err
	}

	switch p.peek().Type {
	case LEFT, RIGHT, INNER, OUTER, CROSS, JOIN:
	default:
		// if no join type is specified, set a default join
		k := &Token{Type: JOIN}
		p.lexer.Tokens = append([]*Token{k}, p.lexer.Tokens...)
	}

	joins, err := p.parseJoins()
	if err != nil {
		return err
	}

	q.Table.Joins = append(q.Table.Joins, joins...)
	return nil
}

func processParams(params []interface{}) {
	for i, v := range params {
		switch t := v.(type) {
		case time.Time:
			// convert all dates for the database into to UTC.
			// The mysql driver does this automatically but the sqlite not.
			params[i] = t.UTC()
		}
	}
}
