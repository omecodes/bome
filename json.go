package bome

import (
	"context"
	"fmt"
)

type JsonValueHolder struct {
	field   string
	dialect string
	tx      *TX
	*DB
}

func (s *JsonValueHolder) Transaction(ctx context.Context) (context.Context, *JsonValueHolder, error) {
	if s.tx != nil {
		tx := transaction(ctx)
		if tx == nil {
			return contextWithTransaction(ctx, s.tx), s, nil
		}
		return ctx, s, nil
	}

	tx := transaction(ctx)
	if tx == nil {
		tx, err := s.DB.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &JsonValueHolder{
			tx:      tx,
			dialect: s.dialect,
		}, nil
	}

	return ctx, &JsonValueHolder{
		tx:      tx,
		dialect: s.dialect,
	}, nil
}

func (s *JsonValueHolder) SwitchToTransactionMode(tx *TX) *JsonValueHolder {
	return &JsonValueHolder{
		tx:      tx,
		dialect: s.dialect,
	}
}

func (s *JsonValueHolder) Client() Client {
	if s.tx != nil {
		return s.tx
	}
	return s.DB
}

func (s *JsonValueHolder) Count() (int, error) {
	o, err := s.Client().QueryFirst("select count(*) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *JsonValueHolder) Size(condition BoolExpr) (int64, error) {
	condition.setDialect(s.dialect)
	o, err := s.Client().QueryFirst("select coalesce(length(value), 0) from $table$ where %s;", IntScanner, condition.sql())
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

	o, err := s.Client().QueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *JsonValueHolder) EditAll(path string, ex Expression) error {
	ex.setDialect(s.dialect)
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(%s, '%s', %s);",
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
	)
	return s.Client().Exec(rawQuery).Error
}

func (s *JsonValueHolder) EditAllMatching(path string, ex Expression, condition BoolExpr) error {
	condition.setDialect(s.dialect)
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(%s, '%s', %s) where %s",
		s.field,
		normalizedJsonPath(path),
		ex.eval(),
		condition.sql(),
	)
	return s.Client().Exec(rawQuery).Error
}

func (s *JsonValueHolder) ExtractAll(path string, condition BoolExpr, scannerName string) (Cursor, error) {
	var rawQuery string
	condition.setDialect(s.dialect)
	if s.dialect == SQLite3 {
		rawQuery = fmt.Sprintf("select json_extract(%s, '%s') from $table$ where %s;",
			s.field,
			path,
			condition.sql(),
		)
	} else {
		rawQuery = fmt.Sprintf("select json_unquote(json_extract(%s, '%s')) from $table$ where %s;",
			s.field,
			path,
			condition.sql(),
		)
	}
	return s.Client().Query(rawQuery, scannerName)
}

func (s *JsonValueHolder) Search(condition BoolExpr, scannerName string) (Cursor, error) {
	condition.setDialect(s.dialect)
	rawQuery := fmt.Sprintf("select * from $table$ where %s;",
		condition.sql(),
	)
	return s.Client().Query(rawQuery, scannerName)
}

func (s *JsonValueHolder) RangeOf(condition BoolExpr, scannerName string, offset, count int) (Cursor, error) {
	condition.setDialect(s.dialect)
	rawQuery := fmt.Sprintf("select * from $table$ where %s limit ?, ?;",
		condition.sql(),
	)
	return s.Client().Query(rawQuery, scannerName, offset, count)
}
