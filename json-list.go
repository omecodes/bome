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

func (l *JSONList) Transaction(ctx context.Context) (context.Context, *JSONList, error) {
	if l.tx != nil {
		tx := transaction(ctx)
		if tx == nil {
			return contextWithTransaction(ctx, l.tx), l, nil
		}
		return ctx, l, nil
	}

	tx := transaction(ctx)
	if tx == nil {
		tx, err := l.DB.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &JSONList{
			JsonValueHolder: &JsonValueHolder{
				tx:      l.tx,
				dialect: l.dialect,
			},
			List: &List{
				tx:        tx,
				tableName: l.tableName,
				dialect:   l.dialect,
			},
			tableName: l.tableName,
			tx:        tx,
			dialect:   l.dialect,
		}, nil
	}

	return ctx, &JSONList{
		JsonValueHolder: &JsonValueHolder{
			tx:      l.tx,
			dialect: l.dialect,
		},
		List: &List{
			tx:        tx,
			tableName: l.tableName,
			dialect:   l.dialect,
		},
		tableName: l.tableName,
		tx:        tx,
		dialect:   l.dialect,
	}, nil
}

func (l *JSONList) SwitchToTransactionMode(tx *TX) *JSONList {
	return &JSONList{
		JsonValueHolder: &JsonValueHolder{
			tx:      l.tx,
			dialect: l.dialect,
		},
		List: &List{
			tx:        tx,
			tableName: l.tableName,
			dialect:   l.dialect,
		},
		tableName: l.tableName,
		tx:        tx,
		dialect:   l.dialect,
	}
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
