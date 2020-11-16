package bome

import (
	"fmt"
	"strings"
)

type Expression interface {
	eval() string
}

type BoolExpr interface {
	sql() string
}

type stringExpression struct {
	value string
}

func (s *stringExpression) eval() string {
	return fmt.Sprintf("'%s'", escaped(s.value))
}

type intExpression struct {
	value int64
}

func (s *intExpression) eval() string {
	return fmt.Sprintf("%d", s.value)
}

type jsonExpression struct {
	expressions []Expression
}

func (s *jsonExpression) eval() string {
	var values []string
	for _, ex := range s.expressions {
		values = append(values, ex.eval())
	}
	return fmt.Sprintf("json_object(%s)", strings.Join(values, ","))
}

func StringExpr(value string) Expression {
	return &stringExpression{value: value}
}

func IntExpr(value int64) Expression {
	return &intExpression{value: value}
}

func RawExpr(sqlRawExpression string) Expression {
	return &rawExpression{rawExpression: sqlRawExpression}
}

func JsonExpr(expressions ...Expression) Expression {
	return &jsonExpression{expressions: expressions}
}

func FieldExpression(name string) Expression {
	return &fieldExpression{
		field: name,
	}
}

type fieldExpression struct {
	field string
}

func (e *fieldExpression) eval() string {
	return fmt.Sprintf("`%s`", e.field)
}

func Or(conditions ...BoolExpr) BoolExpr {
	return &funcCond{
		op:       "or",
		operands: conditions,
	}
}

func And(conditions ...BoolExpr) BoolExpr {
	return &funcCond{
		op:       "and",
		operands: conditions,
	}
}

func Not(condition BoolExpr) BoolExpr {
	return &not{
		condition: condition,
	}
}

func Eq(e Expression) BoolExpr {
	return &eq{
		e: e,
	}
}

func Ne(expression Expression) BoolExpr {
	return &ne{
		e: expression,
	}
}

func Gt(e Expression) BoolExpr {
	return &gt{
		e: e,
	}
}

func Gte(e Expression) BoolExpr {
	return &gte{
		e: e,
	}
}

func Lt(e Expression) BoolExpr {
	return &lt{
		e: e,
	}
}

func Lte(e Expression) BoolExpr {
	return &lte{
		e: e,
	}
}

func Contains(e Expression) BoolExpr {
	return &contains{
		e: e,
	}
}

func StartsWith(e Expression) BoolExpr {
	return &startsWith{
		e: e,
	}
}

func JsonContainsPath(path string) BoolExpr {
	return &jsonContainsPath{
		path: path,
	}
}

func JsonAtContains(path string, e Expression) BoolExpr {
	return &jsonAtContains{
		path: path,
		e:    e,
	}
}

func JsonAtStartsWith(path string, e Expression) BoolExpr {
	return &jsonAtStartWith{
		path: path,
		e:    e,
	}
}

func JsonAtEndsWith(path string, e Expression) BoolExpr {
	return &jsonAtEndsWith{
		path: path,
		e:    e,
	}
}

func JsonAtEq(path string, e Expression) BoolExpr {
	return &jsonAtEquals{
		path:       path,
		expression: e,
	}
}

func JsonAtLt(path string, e Expression) BoolExpr {
	return &jsonAtLt{
		path: path,
		e:    e,
	}
}

func JsonAtLe(path string, e Expression) BoolExpr {
	return &jsonAtLe{
		path: path,
		e:    e,
	}
}

func JsonAtGt(path string, e Expression) BoolExpr {
	return &jsonAtGt{
		path: path,
		e:    e,
	}
}

func JsonAtGe(path string, e Expression) BoolExpr {
	return &jsonAtGe{
		path: path,
		e:    e,
	}
}

func EndsWith(e Expression) BoolExpr {
	return &endsWith{e: e}
}
