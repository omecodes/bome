package bome

import (
	"fmt"
	"strings"
)

type JsonValueHolderTx struct {
	field string
	tx    *TX
}

func (s *JsonValueHolderTx) Client() Client {
	return s.tx
}

func (s *JsonValueHolderTx) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set __value__=json_set(%s, '%s', %s);",
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLExec(rawQuery)
}

func (s *JsonValueHolderTx) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set __value__=json_insert(%s, '%s', %s) where %s",
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLExec(rawQuery)
}

func (s *JsonValueHolderTx) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(%s, '%s')) from $table$ where %s;",
		s.field,
		path,
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName)
}

func (s *JsonValueHolderTx) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s;",
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName)
}

func (s *JsonValueHolderTx) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s limit ?, ?;",
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName, offset, count)
}

func (s *JsonValueHolderTx) Commit() error {
	return s.tx.Commit()
}

func (s *JsonValueHolderTx) Rollback() error {
	return s.tx.Rollback()
}

func (s *JsonValueHolderTx) TX() *TX {
	return s.tx
}
