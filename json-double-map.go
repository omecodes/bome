package bome

import (
	"context"
	"fmt"
)

type JSONDoubleMap struct {
	*DB
	*JsonValueHolder
	*DoubleMap
	tx        *TX
	tableName string
	dialect   string
}

func (s *JSONDoubleMap) Table() string {
	return s.tableName
}

func (s *JSONDoubleMap) Keys() []string {
	return []string{
		"first_key", "second_key",
	}
}

func (s *JSONDoubleMap) Transaction(ctx context.Context) (context.Context, *JSONDoubleMap, error) {
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
		return newCtx, &JSONDoubleMap{
			JsonValueHolder: &JsonValueHolder{
				tx:      s.tx,
				dialect: s.dialect,
			},
			DoubleMap: &DoubleMap{
				tableName: s.tableName,
				tx:        s.tx,
				dialect:   s.dialect,
			},
			tableName: s.tableName,
			tx:        tx,
			dialect:   s.dialect,
		}, nil
	}

	return ctx, &JSONDoubleMap{
		JsonValueHolder: &JsonValueHolder{
			tx:      s.tx,
			dialect: s.dialect,
		},
		DoubleMap: &DoubleMap{
			tableName: s.tableName,
			tx:        s.tx,
			dialect:   s.dialect,
		},
		tableName: s.tableName,
		tx:        tx,
		dialect:   s.dialect,
	}, nil
}

func (s *JSONDoubleMap) Client() Client {
	if s.tx != nil {
		return s.tx
	}
	return s.DB
}

func (s *JSONDoubleMap) EditAt(firstKey, secondKey string, path string, value string) error {
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', \"%s\") where first_key=? and second_key=?;",
		normalizedJsonPath(path),
		value,
	)
	return s.Client().Exec(rawQuery, firstKey, secondKey).Error
}

func (s *JSONDoubleMap) ExtractAt(firstKey, secondKey string, path string) (string, error) {
	var rawQuery string

	if s.dialect == SQLite3 {
		rawQuery = fmt.Sprintf(
			"select json_extract(value, '%s') from $table$ where first_key=? and second_key=?;", path)
	} else {
		rawQuery = fmt.Sprintf(
			"select json_unquote(json_extract(value, '%s')) from $table$ where first_key=? and second_key=?;", path)
	}

	o, err := s.Client().QueryFirst(rawQuery, StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}
