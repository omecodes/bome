package bome

import (
	"context"
	"fmt"
)

type JSONList struct {
	*List
	*JsonValueHolder
	*DB
	tx        *TX
	tableName string
	dialect   string
}

func (l *JSONList) Table() string {
	return l.tableName
}

func (l *JSONList) Keys() []string {
	return []string{
		"ind",
	}
}

func (s *JSONList) Transaction(ctx context.Context) (context.Context, *JSONList, error) {
	tx := transaction(ctx)
	if tx == nil {
		if s.tx != nil {
			return contextWithTransaction(ctx, s.tx), s, nil
		}

		var err error
		tx, err = s.DB.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &JSONList{
			JsonValueHolder: &JsonValueHolder{
				field:   "value",
				dialect: s.dialect,
				tx:      tx,
			},
			List: &List{
				tableName: s.tableName,
				dialect:   s.dialect,
				tx:        tx,
			},
			tableName: s.tableName,
			tx:        tx,
			dialect:   s.dialect,
		}, nil
	}

	if s.tx != nil {
		if s.tx.db.sqlDb != tx.db.sqlDb {
			newCtx := ContextWithCommitActions(ctx, tx.Commit)
			newCtx = ContextWithRollbackActions(newCtx, tx.Rollback)

			var err error
			tx, err = s.DB.BeginTx()
			if err != nil {
				return ctx, nil, err
			}

			return contextWithTransaction(newCtx, tx), &JSONList{
				JsonValueHolder: &JsonValueHolder{
					field:   "value",
					dialect: s.dialect,
					tx:      tx,
				},
				List: &List{
					tableName: s.tableName,
					dialect:   s.dialect,
					tx:        tx,
				},
				tableName: s.tableName,
				tx:        tx,
				dialect:   s.dialect,
			}, nil
		}

		return ctx, &JSONList{
			JsonValueHolder: &JsonValueHolder{
				field:   "value",
				dialect: s.dialect,
				tx:      tx,
			},
			List: &List{
				tableName: s.tableName,
				dialect:   s.dialect,
				tx:        tx,
			},
			tableName: s.tableName,
			tx:        tx,
			dialect:   s.dialect,
		}, nil
	}

	tx, err := s.DB.BeginTx()
	if err != nil {
		return ctx, nil, err
	}

	newCtx := contextWithTransaction(ctx, tx)
	return newCtx, &JSONList{
		JsonValueHolder: &JsonValueHolder{
			field:   "value",
			dialect: s.dialect,
			tx:      tx,
		},
		List: &List{
			tableName: s.tableName,
			dialect:   s.dialect,
			tx:        tx,
		},
		tableName: s.tableName,
		tx:        tx,
		dialect:   s.dialect,
	}, nil
}

func (l *JSONList) Client() Client {
	if l.tx != nil {
		return l.tx
	}
	return l.DB
}

func (l *JSONList) EditAt(index int64, path string, sqlValue string) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s) where ind=?;", path, sqlValue)
	return l.Client().Exec(rawQuery, index).Error
}

func (l *JSONList) ExtractAt(index int64, path string) (string, error) {
	var rawQuery string

	if l.dialect == SQLite3 {
		rawQuery = fmt.Sprintf(
			"select json_extract(value, '%s') from $table$ where ind=?;", path)
	} else {
		rawQuery = fmt.Sprintf(
			"select json_unquote(json_extract(value, '%s')) from $table$ where ind=?;", path)
	}
	o, err := l.Client().QueryFirst(rawQuery, StringScanner, index)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}
