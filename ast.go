//go:generate stringer -type=Type

package goql

import (
	"bytes"
	"fmt"
)

type Position struct {
	Line   int
	Column int
	Length int
}

func (p Position) String() string {
	var buf bytes.Buffer

	if p.Line > 0 {
		fmt.Fprintf(&buf, "line %d ", p.Line+1)
	}

	fmt.Fprintf(&buf, "column %d", p.Column)
	return buf.String()
}

type Token struct {
	Type Type
	Str  string
	Pos  Position
}

type Error struct {
	Pos     Position
	Message string
	Token   string
}

func (e *Error) Position() Position {
	return e.Pos
}

func (e *Error) Error() string {
	return e.Message
}

type ParamsQuery interface {
	GetParams() []interface{}
}

// All node types implement the Node interface.
type Node interface {
	Position() Position
}

// All expression nodes implement the Expr interface.
type Expr interface {
	Node
	exprNode()
}

// All SQL queries implement the Query interface.
type Query interface {
	Node
	queryNode()
}

type SqlFrom interface {
	Node
	fromNode()
}

type FromAsExpr struct {
	From  *ParenExpr
	Alias string
}

func (a *FromAsExpr) Position() Position {
	return a.From.Position()
}
func (a *FromAsExpr) fromNode() {}

type SelectColumnExpr struct {
	Expr  Expr
	Alias string
}

func (q *SelectColumnExpr) Position() Position {
	return q.Expr.Position()
}

func (q *SelectColumnExpr) exprNode() {}

type AllColumnsExpr struct {
	Table string
	Pos   Position
}

func (q *AllColumnsExpr) Position() Position {
	return q.Pos
}

func (q *AllColumnsExpr) exprNode() {}

type OrderColumn struct {
	Expr Expr
	Type Type
}

type ParameterExpr struct {
	Pos  Position
	Name string
}

func (q *ParameterExpr) Position() Position {
	return q.Pos
}

func (q *ParameterExpr) exprNode() {}

type TableName struct {
	Pos      Position
	Database string
	Name     string
}

type ColumnValue struct {
	Pos   Position
	Table string
	Name  string
	Expr  Expr
}

type Table struct {
	Pos      Position
	Name     string
	Database string
	Alias    string
	Joins    []*Join
}

// RefName returns the name as how it should be referenced.
// If it has an alias, the alias, and if not, the name.
func (q *Table) RefName() string {
	if q.Alias != "" {
		return q.Alias
	}
	return q.Name
}

func (q *Table) FullName() string {
	if q.Alias != "" {
		return q.Alias
	}

	if q.Database != "" {
		return q.Database + "." + q.Name
	}

	return q.Name
}

func (q *Table) Position() Position {
	return q.Pos
}

func (q *Table) fromNode() {}

type Join struct {
	Pos      Position
	Type     Type
	Table    string
	Alias    string
	Database string
	On       Expr
}

type WherePart struct {
	Pos  Position
	Expr Expr
}

type Limit struct {
	Pos      Position
	RowCount Expr
	Offset   Expr
}

type ColumnType byte

const (
	Int ColumnType = iota
	Decimal
	Char
	Varchar
	Text
	MediumText
	Bool
	Blob
	DatTime
)

type CreateColumn struct {
	Name     string
	Type     ColumnType
	Nullable bool
	Size     string
	Decimals string
	Key      bool
	Default  string
}

type Constraint struct {
	Name    string
	Type    string
	Columns []string
}

func (Constraint) constraintNode() {}

type ForeginKey struct {
	Name          string
	Column        string
	RefTable      string
	RefColumn     string
	DeleteCascade bool
}

func (ForeginKey) constraintNode() {}

type CreateTableConstraint interface {
	constraintNode()
}

type Column struct {
	Table string
	Name  string
}

type CreateDatabaseQuery struct {
	Pos         Position
	Name        string
	IfNotExists bool
}

func (q *CreateDatabaseQuery) Position() Position {
	return q.Pos
}

func (q *CreateDatabaseQuery) queryNode() {}

type CreateTableQuery struct {
	Pos         Position
	Name        string
	Columns     []*CreateColumn
	Constraints []CreateTableConstraint
	IfNotExists bool
}

func (q *CreateTableQuery) Position() Position {
	return q.Pos
}

func (q *CreateTableQuery) queryNode() {}

type ShowQuery struct {
	Pos      Position
	Type     string
	Database string
	Table    string
}

func (q *ShowQuery) Position() Position {
	return q.Pos
}

func (q *ShowQuery) queryNode() {}

type DropDatabaseQuery struct {
	Pos      Position
	Database string
	IfExists bool
}

func (q *DropDatabaseQuery) Position() Position {
	return q.Pos
}

func (q *DropDatabaseQuery) queryNode() {}

type DropTableQuery struct {
	Pos      Position
	Database string
	Table    string
	IfExists bool
}

func (q *DropTableQuery) Position() Position {
	return q.Pos
}

func (q *DropTableQuery) queryNode() {}

type AlterDropQuery struct {
	Pos      Position
	Database string
	Table    string
	Type     string
	Item     string
}

func (q *AlterDropQuery) Position() Position {
	return q.Pos
}

func (q *AlterDropQuery) queryNode() {}

type AddColumnQuery struct {
	Pos      Position
	Database string
	Table    string
	Column   *CreateColumn
}

func (q *AddColumnQuery) Position() Position {
	return q.Pos
}

func (q *AddColumnQuery) queryNode() {}

type RenameColumnQuery struct {
	Pos      Position
	Database string
	Table    string
	Name     string
	Column   *CreateColumn
}

func (q *RenameColumnQuery) Position() Position {
	return q.Pos
}

func (q *RenameColumnQuery) queryNode() {}

type ModifyColumnQuery struct {
	Pos      Position
	Database string
	Table    string
	Column   *CreateColumn
}

func (q *ModifyColumnQuery) Position() Position {
	return q.Pos
}

func (q *ModifyColumnQuery) queryNode() {}

type AddConstraintQuery struct {
	Pos      Position
	Type     string
	Database string
	Table    string
	Name     string
	Columns  []*ColumnNameExpr
}

func (q *AddConstraintQuery) Position() Position {
	return q.Pos
}

func (q *AddConstraintQuery) queryNode() {}

type AddFKQuery struct {
	Pos           Position
	Type          string
	Database      string
	Table         string
	Name          string
	Column        string
	RefDatabase   string
	RefTable      string
	RefColumn     string
	DeleteCascade bool
}

func (q *AddFKQuery) Position() Position {
	return q.Pos
}

func (q *AddFKQuery) queryNode() {}

type SelectQuery struct {
	Pos         Position
	Distinct    bool
	ForUpdate   bool
	Columns     []Expr
	From        []SqlFrom
	WherePart   *WherePart
	GroupByPart []Expr
	HavingPart  *WherePart
	OrderByPart []*OrderColumn
	LimitPart   *Limit
	UnionPart   []*SelectQuery
	Params      []interface{}
}

func (q *SelectQuery) GetParams() []interface{} {
	return q.Params
}

func (q *SelectQuery) Position() Position {
	return q.Pos
}

// **DEPRECATE**
func (q *SelectQuery) RemoveLeftJoins() {
	if len(q.From) != 1 {
		return
	}

	from, ok := q.From[0].(*Table)
	if !ok {
		return
	}

	for i := len(from.Joins) - 1; i >= 0; i-- {
		j := from.Joins[i]
		if j.Type == LEFT {
			from.Joins = append(from.Joins[:i], from.Joins[i+1:]...)
		}
	}
}

func (q *SelectQuery) queryNode() {}

type InsertQuery struct {
	Pos     Position
	Table   *TableName
	Columns []*ColumnNameExpr
	Values  []Expr
	Params  []interface{}
	Select  *SelectQuery // in case is a insert from a select
}

func (q *InsertQuery) Position() Position {
	return q.Pos
}

func (q *InsertQuery) queryNode() {}

type UpdateQuery struct {
	Pos       Position
	Table     *Table
	Columns   []ColumnValue
	WherePart *WherePart
	LimitPart *Limit
	Params    []interface{}
}

func (q *UpdateQuery) Position() Position {
	return q.Pos
}

func (q *UpdateQuery) queryNode() {}

type DeleteQuery struct {
	Pos       Position
	Alias     []string
	Table     *Table
	WherePart *WherePart
	LimitPart *Limit
	Params    []interface{}
}

func (q *DeleteQuery) Position() Position {
	return q.Pos
}

func (q *DeleteQuery) queryNode() {}

func (q *SelectQuery) exprNode() {}

type ColumnNameExpr struct {
	Pos   Position
	Name  string
	Table string
	Alias string
}

func (i *ColumnNameExpr) Position() Position {
	return i.Pos
}
func (i *ColumnNameExpr) exprNode() {}

type BetweenExpr struct {
	LExpr Expr
	RExpr Expr
}

func (i *BetweenExpr) Position() Position {
	return i.LExpr.Position()
}
func (i *BetweenExpr) exprNode() {}

type InExpr struct {
	LParen Position
	Values []Expr
	RParen Position
}

func (i *InExpr) Position() Position {
	return i.LParen
}
func (i *InExpr) exprNode() {}

type IdentExpr struct {
	Pos  Position
	Name string
}

func (i *IdentExpr) Position() Position {
	return i.Pos
}
func (i *IdentExpr) exprNode() {}

type ConstantExpr struct {
	Pos   Position
	Kind  Type
	Value string
}

func (i *ConstantExpr) Position() Position {
	return i.Pos
}
func (i *ConstantExpr) exprNode() {}

type UnaryExpr struct {
	Pos      Position
	Operator Type
	Operand  Expr
}

func (i *UnaryExpr) Position() Position {
	return i.Pos
}
func (i *UnaryExpr) exprNode() {}

type ParenExpr struct {
	LParen Position
	X      Expr
	RParen Position
}

func (i *ParenExpr) Position() Position {
	return i.X.Position()
}
func (i *ParenExpr) exprNode() {}
func (q *ParenExpr) fromNode() {}

type BinaryExpr struct {
	Operator Type
	Left     Expr
	Right    Expr
}

func (i *BinaryExpr) Position() Position {
	return i.Left.Position()
}
func (i *BinaryExpr) exprNode() {}

type CallExpr struct {
	Pos  Position
	Name string
	Args []Expr
}

func (i *CallExpr) Position() Position {
	return i.Pos
}
func (i *CallExpr) exprNode() {}

type GroupConcatExpr struct {
	Pos         Position
	Distinct    bool
	Expressions []Expr
	OrderByPart []*OrderColumn
	Separator   string
}

func (i *GroupConcatExpr) Position() Position {
	return i.Pos
}
func (i *GroupConcatExpr) exprNode() {}

var reservedWords = map[string]Type{
	"CREATE":     CREATE,
	"SHOW":       SHOW,
	"ALTER":      ALTER,
	"DROP":       DROP,
	"TABLE":      TABLE,
	"DATABASE":   DATABASE,
	"NOT":        NOT,
	"EXISTS":     EXISTS,
	"CONSTRAINT": CONSTRAINT,
	"INT":        INTEGER,
	"DECIMAL":    DECIMAL,
	"CHAR":       CHAR,
	"VARCHAR":    VARCHAR,
	"TEXT":       TEXT,
	"MEDIUMTEXT": MEDIUMTEXT,
	"BOOL":       BOOL,
	"BLOB":       BLOB,
	"DATETIME":   DATETIME,
	"DEFAULT":    DEFAULT,
	"SELECT":     SELECT,
	"DISTINCT":   DISTINCT,
	"INSERT":     INSERT,
	"INTO":       INTO,
	"VALUES":     VALUES,
	"UPDATE":     UPDATE,
	"SET":        SET,
	"DELETE":     DELETE,
	"FROM":       FROM,
	"WHERE":      WHERE,
	"GROUP":      GROUP,
	"HAVING":     HAVING,
	"JOIN":       JOIN,
	"LEFT":       LEFT,
	"RIGHT":      RIGHT,
	"INNER":      INNER,
	"OUTER":      OUTER,
	"CROSS":      CROSS,
	"ON":         ON,
	"AS":         AS,
	"IN":         IN,
	"BETWEEN":    BETWEEN,
	"IS":         IS,
	"LIKE":       LIKE,
	"NOTLIKE":    NOTLIKE,
	"ORDER":      ORDER,
	"BY":         BY,
	"ASC":        ASC,
	"DESC":       DESC,
	"RANDOM":     RANDOM,
	"LIMIT":      LIMIT,
	"AND":        AND,
	"OR":         OR,
	"NULL":       NULL,
	"TRUE":       TRUE,
	"FALSE":      FALSE,
	"FOR":        FOR,
	"UNION":      UNION,
}

type Type byte

const (
	NOTSET Type = iota
	ERROR
	EOF
	COMMENT // --

	// Keywords
	CREATE
	SHOW
	DROP
	ALTER
	TABLE
	DATABASE
	NOT
	EXISTS
	CONSTRAINT
	INTEGER
	DECIMAL
	CHAR
	VARCHAR
	TEXT
	MEDIUMTEXT
	BOOL
	BLOB
	DATETIME
	DEFAULT

	SELECT
	DISTINCT
	INSERT
	INTO
	VALUES
	UPDATE
	SET
	DELETE
	FROM
	WHERE
	GROUP
	HAVING
	JOIN
	LEFT
	RIGHT
	INNER
	OUTER
	CROSS
	ON
	AS
	IN
	NOTIN
	BETWEEN
	LIKE
	IS
	ISNOT
	NOTLIKE
	ORDER
	BY
	ASC
	DESC
	RANDOM
	LIMIT
	UNION
	AND
	OR
	NULL
	TRUE
	FALSE

	FOR

	IDENT  //  fields, tables...
	INT    // 12345
	FLOAT  // 123.45
	STRING // "abc"

	// Operators and delimiters
	ADD // +
	SUB // -
	MUL // *
	DIV // /
	MOD // %

	LSF // << left shift
	RSF // >> right shift
	ANB // &  binary AND

	EQL // =
	LSS // <
	GTR // >
	NT  // !

	NEQ // !=
	LEQ // <=
	GEQ // >=

	LPAREN // (
	LBRACK // [
	LBRACE // {
	COMMA  // ,
	PERIOD // .

	RPAREN    // )
	COLON     // ;
	SEMICOLON // ;
	QUESTION  // ?
)
