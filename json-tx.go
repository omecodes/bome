package bome

import (
	"fmt"
)

type JsonValueHolderTx struct {
	tableName string
	field     string
	tx        *TX
}

func (s *JsonValueHolderTx) Client() Client {
	return s.tx
}

func (s *JsonValueHolderTx) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update "+s.tableName+" set %s=json_set(%s, '%s', %s);",
		s.field,
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
	)
	return s.Client().SQLExec(rawQuery)
}

func (s *JsonValueHolderTx) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	rawQuery := fmt.Sprintf(
		"update "+s.tableName+" set %s=json_insert(%s, '%s', %s) where %s",
		s.field,
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return s.Client().SQLExec(rawQuery)
}

func (s *JsonValueHolderTx) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(%s, '%s')) from "+s.tableName+" where %s;",
		s.field,
		path,
		condition.sql(),
	)
	return s.Client().SQLQuery(rawQuery, scannerName)
}

func (s *JsonValueHolderTx) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from "+s.tableName+" where %s;",
		condition.sql(),
	)
	return s.Client().SQLQuery(rawQuery, scannerName)
}

func (s *JsonValueHolderTx) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from "+s.tableName+" where %s limit ?, ?;",
		condition.sql(),
	)
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
