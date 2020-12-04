package bome

import (
	"context"
	"fmt"
	"strings"
)

func NewJsonValueHolder(field string, db *Bome) *JsonValueHolder {
	return &JsonValueHolder{
		field: field,
		Bome:  db,
	}
}

type JsonValueHolder struct {
	field string
	*Bome
}

func (s *JsonValueHolder) Transaction(ctx context.Context) (context.Context, *JsonValueHolderTx, error) {
	tx := transaction(ctx)
	if tx == nil {
		tx, err := s.Bome.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &JsonValueHolderTx{
			field: s.field,
			tx:    tx,
		}, nil
	}

	return ctx, &JsonValueHolderTx{
		field: s.field,
		tx:    tx.clone(s.Bome),
	}, nil
}

func (s *JsonValueHolder) BeginTransaction() (*JsonValueHolderTx, error) {
	tx, err := s.Bome.BeginTx()
	if err != nil {
		return nil, err
	}

	return &JsonValueHolderTx{
		field: s.field,
		tx:    tx,
	}, nil
}

func (s *JsonValueHolder) ContinueTransaction(tx *TX) *JsonValueHolderTx {
	return &JsonValueHolderTx{
		field: s.field,
		tx:    tx.clone(s.Bome),
	}
}

func (s *JsonValueHolder) Client() Client {
	return s.Bome
}

func (s *JsonValueHolder) Count() (int, error) {
	o, err := s.Client().SQLQueryFirst("select count(*) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *JsonValueHolder) Size(condition BoolExpr) (int64, error) {
	o, err := s.Client().SQLQueryFirst("select length(value) from $table$ where %s;", IntScanner, condition.sql())
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *JsonValueHolder) TotalSize() (int64, error) {
	count, err := s.Count()
	if err != nil {
		return 0, nil
	}

	if count == 0 {
		return 0, nil
	}

	o, err := s.Client().SQLQueryFirst("select sum(length(value)) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *JsonValueHolder) EditAll(path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set __value__=json_set(%s, '%s', %s);",
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLExec(rawQuery)
}

func (s *JsonValueHolder) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
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

func (s *JsonValueHolder) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select json_unquote(json_extract(%s, '%s')) from $table$ where %s;",
		s.field,
		path,
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName)
}

func (s *JsonValueHolder) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s;",
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName)
}

func (s *JsonValueHolder) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	rawQuery := fmt.Sprintf("select * from $table$ where %s limit ?, ?;",
		condition.sql(),
	)
	rawQuery = strings.Replace(rawQuery, "__value__", s.field, -1)
	return s.Client().SQLQuery(rawQuery, scannerName, offset, count)
}
