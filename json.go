package bome

import (
	"fmt"
	"strings"
)

type JsonValueHolder interface {
	EditAll(path string, ex Expression) error
	EditAllMatching(path string, ex Expression, condition BoolExpr) error
	ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error)
	Search(condition BoolExpr, scannerName string) (Cursor, error)
	RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error)
}

func NewJsonValueHolder(table string, field string, db *Bome) JsonValueHolder {
	return &jsonValueHolder{
		field: field,
		table: table,
		Bome:  db,
	}
}

type jsonValueHolder struct {
	field string
	table string
	*Bome
}

func (s *jsonValueHolder) BeginTransaction() (JsonValueHolderTransaction, error) {
	tx, err := s.Bome.BeginTx()
	if err != nil {
		return nil, err
	}

	return &txJsonValueHolder{
		JsonValueHolder: s,
		tx:              tx,
	}, nil
}

func (s *jsonValueHolder) Client() Client {
	return s.Bome
}

func (s *jsonValueHolder) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update %s set __value__=json_set(%s, '%s', %s);",
		s.table,
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLExec(rawQuery)
}

func (s *jsonValueHolder) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update %s set __value__=json_insert(%s, '%s', %s) where %s",
		s.table,
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLExec(rawQuery)
}

func (s *jsonValueHolder) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(%s, '%s')) from %s where %s;",
		s.field,
		path,
		s.table,
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName)
}

func (s *jsonValueHolder) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from %s where %s;",
		s.table,
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName)
}

func (s *jsonValueHolder) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from %s where %s limit ?, ?;",
		s.table,
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName, offset, count)
}
