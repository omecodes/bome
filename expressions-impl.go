package bome

import (
	"log"
	"strings"
)

type funcCond struct {
	op       string
	operands []BoolExpr
	dialectValue
}

func (fc *funcCond) sql() string {
	var sqls []string
	for _, cond := range fc.operands {
		cond.setDialect(fc.dialect)
		sqls = append(sqls, "("+cond.sql()+")")
	}
	return strings.Join(sqls, " "+fc.op+" ")
}

type contains struct {
	e Expression
	dialectValue
}

func (c *contains) sql() string {
	c.e.setDialect(c.dialect)
	expr := c.e.eval()
	expr = expr[1:]
	expr = expr[:len(expr)-1]
	expr = "'%" + expr + "%'"

	return "(value like " + expr + ")"
}

type startsWith struct {
	e Expression
	dialectValue
}

func (s *startsWith) sql() string {
	s.e.setDialect(s.dialect)
	expr := s.e.eval()
	expr = strings.TrimSuffix(expr, "'")
	expr = expr + "%'"
	return "(value like " + expr + ")"
}

type endsWith struct {
	e Expression
	dialectValue
}

func (e *endsWith) sql() string {
	e.e.setDialect(e.dialect)
	expr := e.e.eval()
	expr = "'%" + expr[1:]
	return "(value like " + expr + ")"
}

type jsonContainsPath struct {
	path string
	dialectValue
}

func (c *jsonContainsPath) sql() string {
	if c.dialect == SQLite3 {
		return "(json_quote(json_extract(value, '" + c.path + "'))!='null')"
	}
	return "(json_contains_path(value, 'one',  '" + c.path + "'))"
}

type jsonAtEquals struct {
	path string
	e    Expression
	dialectValue
}

func (c *jsonAtEquals) sql() string {
	c.e.setDialect(c.dialect)
	expr := c.e.eval()
	expr = expr[1:]
	expr = expr[:len(expr)-1]
	expr = "'" + expr + "'"

	builder := strings.Builder{}
	if c.dialect == SQLite3 {
		builder.WriteString("(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("') = ")
	} else {
		builder.WriteString("(json_unquote(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("')) = ")
	}
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtContains struct {
	path string
	e    Expression
	dialectValue
}

func (c *jsonAtContains) sql() string {
	c.e.setDialect(c.dialect)
	expr := c.e.eval()
	expr = expr[1:]
	expr = expr[:len(expr)-1]
	expr = "'%" + expr + "%'"

	builder := strings.Builder{}
	if c.dialect == SQLite3 {
		builder.WriteString("(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("') like ")
	} else {
		builder.WriteString("(json_unquote(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("')) like ")
	}

	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtStartWith struct {
	path string
	e    Expression
	dialectValue
}

func (s *jsonAtStartWith) sql() string {
	s.e.setDialect(s.dialect)
	expr := s.e.eval()
	expr = expr[:len(expr)-1]
	expr = expr + "%'"

	builder := strings.Builder{}
	if s.dialect == SQLite3 {
		builder.WriteString("(json_extract(value,'")
		builder.WriteString(s.path)
		builder.WriteString("') like ")
	} else {
		builder.WriteString("(json_unquote(json_extract(value,'")
		builder.WriteString(s.path)
		builder.WriteString("')) like ")
	}
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtEndsWith struct {
	path string
	e    Expression
	dialectValue
}

func (e *jsonAtEndsWith) sql() string {
	e.e.setDialect(e.dialect)
	expr := e.e.eval()
	expr = "'%" + expr[1:]

	builder := strings.Builder{}
	if e.dialect == SQLite3 {
		builder.WriteString("(json_extract(value,'")
		builder.WriteString(e.path)
		builder.WriteString("') like ")
	} else {
		builder.WriteString("(json_unquote(json_extract(value,'")
		builder.WriteString(e.path)
		builder.WriteString("')) like ")
	}
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtLt struct {
	path string
	e    Expression
	dialectValue
}

func (c *jsonAtLt) sql() string {
	c.e.setDialect(c.dialect)
	expr := c.e.eval()
	builder := strings.Builder{}
	if c.dialect == SQLite3 {
		builder.WriteString("(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("')<")
	} else {
		builder.WriteString("(json_unquote(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("'))<")
	}
	builder.WriteString(expr)
	builder.WriteString(")")

	log.Println(builder.String())
	return builder.String()
}

type jsonAtLe struct {
	path string
	e    Expression
	dialectValue
}

func (c *jsonAtLe) sql() string {
	c.e.setDialect(c.dialect)
	expr := c.e.eval()
	builder := strings.Builder{}
	if c.dialect == SQLite3 {
		builder.WriteString("(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("') <= ")
	} else {
		builder.WriteString("(json_unquote(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("')) <= ")
	}
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtGt struct {
	path string
	e    Expression
	dialectValue
}

func (c *jsonAtGt) sql() string {
	c.e.setDialect(c.dialect)
	expr := c.e.eval()
	builder := strings.Builder{}
	if c.dialect == SQLite3 {
		builder.WriteString("(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("') > ")
	} else {
		builder.WriteString("(json_unquote(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("')) > ")
	}
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtGe struct {
	path string
	e    Expression
	dialectValue
}

func (c *jsonAtGe) sql() string {
	c.e.setDialect(c.dialect)
	expr := c.e.eval()
	builder := strings.Builder{}
	if c.dialect == SQLite3 {
		builder.WriteString("(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("') >= ")
	} else {
		builder.WriteString("(json_unquote(json_extract(value,'")
		builder.WriteString(c.path)
		builder.WriteString("')) >= ")
	}
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type not struct {
	e BoolExpr
	dialectValue
}

func (n *not) sql() string {
	n.e.setDialect(n.dialect)
	return "not (" + n.e.sql() + ")"
}

type eq struct {
	e Expression
	dialectValue
}

func (e *eq) sql() string {
	e.e.setDialect(e.dialect)
	return "value = " + e.e.eval()
}

type ne struct {
	e Expression
	dialectValue
}

func (n *ne) sql() string {
	n.e.setDialect(n.dialect)
	return "value != " + n.e.eval()
}

type gt struct {
	e Expression
	dialectValue
}

func (g *gt) sql() string {
	g.e.setDialect(g.dialect)
	return "value > " + g.e.eval()
}

type gte struct {
	e Expression
	dialectValue
}

func (g *gte) sql() string {
	g.e.setDialect(g.dialect)
	return "value >= " + g.e.eval()
}

type lt struct {
	e Expression
	dialectValue
}

func (l *lt) sql() string {
	l.e.setDialect(l.dialect)
	return "value < " + l.e.eval()
}

type lte struct {
	e Expression
	dialectValue
}

func (l *lte) sql() string {
	l.e.setDialect(l.dialect)
	return "value <= " + l.e.eval()
}

type trueExpr struct {
	dialectValue
}

func (_ *trueExpr) sql() string {
	return "(1)"
}

type falseExpr struct {
	dialectValue
}

func (_ *falseExpr) sql() string {
	return "(0)"
}

type rawExpression struct {
	rawExpression string
	dialectValue
}

func (r *rawExpression) eval() string {
	return r.rawExpression
}
