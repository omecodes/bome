package bome

import (
	"strings"
)

type funcCond struct {
	op       string
	operands []BoolExpr
}

func (fc *funcCond) sql() string {
	var sqls []string
	for _, cond := range fc.operands {
		sqls = append(sqls, "("+cond.sql()+")")
	}
	return strings.Join(sqls, " "+fc.op+" ")
}

type contains struct {
	e Expression
}

func (c *contains) sql() string {
	expr := c.e.eval()
	expr = expr[1:]
	expr = expr[:len(expr)-1]
	expr = "'%" + expr + "%'"

	return "(__value__ like " + expr + ")"
}

type startsWith struct {
	e Expression
}

func (s *startsWith) sql() string {
	expr := s.e.eval()
	if strings.HasSuffix(expr, "'") {
		expr = expr[:len(expr)-1]
	}
	expr = expr + "%'"
	return "(__value__ like " + expr + ")"
}

type endsWith struct {
	e Expression
}

func (e *endsWith) sql() string {
	expr := e.e.eval()
	expr = "'%" + expr[1:]
	return "(__value__ like " + expr + ")"
}

type jsonContainsPath struct {
	path string
}

func (c *jsonContainsPath) sql() string {
	return "(json_contains_path(__value__, 'one',  '" + c.path + "'))"
}

type jsonAtEquals struct {
	path       string
	expression Expression
}

func (c *jsonAtEquals) sql() string {
	expr := c.expression.eval()
	expr = expr[1:]
	expr = expr[:len(expr)-1]
	expr = "'" + expr + "'"

	builder := strings.Builder{}
	builder.WriteString("(json_unquote(json_extract(__value__,'")
	builder.WriteString(c.path)
	builder.WriteString("')) = ")
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtContains struct {
	path string
	e    Expression
}

func (c *jsonAtContains) sql() string {
	expr := c.e.eval()
	expr = expr[1:]
	expr = expr[:len(expr)-1]
	expr = "'%" + expr + "%'"

	builder := strings.Builder{}
	builder.WriteString("(json_unquote(json_extract(__value__,'")
	builder.WriteString(c.path)
	builder.WriteString("')) like ")
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtStartWith struct {
	path string
	e    Expression
}

func (s *jsonAtStartWith) sql() string {
	expr := s.e.eval()
	expr = expr[:len(expr)-1]
	expr = expr + "%'"

	builder := strings.Builder{}
	builder.WriteString("(json_unquote(json_extract(__value__,'")
	builder.WriteString(s.path)
	builder.WriteString("')) like ")
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtEndsWith struct {
	path string
	e    Expression
}

func (e *jsonAtEndsWith) sql() string {
	expr := e.e.eval()
	expr = "'%" + expr[1:]

	builder := strings.Builder{}
	builder.WriteString("(json_unquote(json_extract(__value__,'")
	builder.WriteString(e.path)
	builder.WriteString("')) like ")
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtLt struct {
	path string
	e    Expression
}

func (c *jsonAtLt) sql() string {
	expr := c.e.eval()
	builder := strings.Builder{}
	builder.WriteString("(json_unquote(json_extract(__value__,'")
	builder.WriteString(c.path)
	builder.WriteString("')) < ")
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtLe struct {
	path string
	e    Expression
}

func (c *jsonAtLe) sql() string {
	expr := c.e.eval()
	builder := strings.Builder{}
	builder.WriteString("(json_unquote(json_extract(__value__,'")
	builder.WriteString(c.path)
	builder.WriteString("')) <= ")
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtGt struct {
	path string
	e    Expression
}

func (c *jsonAtGt) sql() string {
	expr := c.e.eval()
	builder := strings.Builder{}
	builder.WriteString("(json_unquote(json_extract(__value__,'")
	builder.WriteString(c.path)
	builder.WriteString("')) > ")
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type jsonAtGe struct {
	path string
	e    Expression
}

func (c *jsonAtGe) sql() string {
	expr := c.e.eval()
	builder := strings.Builder{}
	builder.WriteString("(json_unquote(json_extract(__value__,'")
	builder.WriteString(c.path)
	builder.WriteString("')) >= ")
	builder.WriteString(expr)
	builder.WriteString(")")
	return builder.String()
}

type not struct {
	condition BoolExpr
}

func (n *not) sql() string {
	return "not (" + n.condition.sql() + ")"
}

type eq struct {
	e Expression
}

func (e *eq) sql() string {
	return "__value__ = " + e.e.eval()
}

type ne struct {
	e Expression
}

func (n *ne) sql() string {
	return "__value__ != " + n.e.eval()
}

type gt struct {
	e Expression
}

func (g *gt) sql() string {
	return "__value__ > " + g.e.eval()
}

type gte struct {
	e Expression
}

func (g *gte) sql() string {
	return "__value__ >= " + g.e.eval()
}

type lt struct {
	e Expression
}

func (l *lt) sql() string {
	return "__value__ < " + l.e.eval()
}

type lte struct {
	e Expression
}

func (l *lte) sql() string {
	return "__value__ <= " + l.e.eval()
}

type rawExpression struct {
	rawExpression string
}

func (r *rawExpression) eval() string {
	return r.rawExpression
}
