package bome

import (
	"context"
	"fmt"
)

type JSONMappingList struct {
	tableName string
	dialect   string
	*DB
	*JsonValueHolder
	*MappingList
	tx *TX
}

func (l *JSONMappingList) Table() string {
	return l.tableName
}

func (l *JSONMappingList) Keys() []string {
	return []string{
		"first_key", "second_key",
	}
}

func (l *JSONMappingList) Transaction(ctx context.Context) (context.Context, *JSONMappingList, error) {
	tx := transaction(ctx)
	if tx == nil {
		if l.tx != nil {
			return contextWithTransaction(ctx, l.tx.New(l.DB)), l, nil
		}

		var err error
		tx, err = l.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &JSONMappingList{
			JsonValueHolder: &JsonValueHolder{
				dialect: l.dialect,
				tx:      tx,
			},
			MappingList: &MappingList{
				tableName: l.tableName,
				dialect:   l.dialect,
				tx:        tx,
			},
			tableName: l.tableName,
			tx:        tx,
			dialect:   l.dialect,
		}, nil
	}

	if l.tx != nil {
		if l.tx.db.sqlDb != tx.db.sqlDb {
			newCtx := ContextWithCommitActions(ctx, tx.Commit)
			newCtx = ContextWithRollbackActions(newCtx, tx.Rollback)
			return contextWithTransaction(newCtx, l.tx.New(l.DB)), l, nil
		}
		return ctx, l, nil
	}
	tx = tx.New(l.DB)
	newCtx := contextWithTransaction(ctx, tx)
	return newCtx, &JSONMappingList{
		JsonValueHolder: &JsonValueHolder{
			dialect: l.dialect,
			tx:      tx,
		},
		MappingList: &MappingList{
			tableName: l.tableName,
			dialect:   l.dialect,
			tx:        tx,
		},
		tableName: l.tableName,
		tx:        tx,
		dialect:   l.dialect,
	}, nil
}

func (l *JSONMappingList) Client() Client {
	if l.tx != nil {
		return l.tx
	}
	return l.DB
}

func (l *JSONMappingList) EditAt(firstKey, secondKey string, path string, value string) error {
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', \"%s\") where first_key=? and second_key=?;",
		normalizedJsonPath(path),
		value,
	)
	return l.Client().Exec(rawQuery, firstKey, secondKey).Error
}

func (l *JSONMappingList) ExtractAt(firstKey, secondKey string, path string) (string, error) {
	var rawQuery string

	if l.dialect == SQLite3 {
		rawQuery = fmt.Sprintf(
			"select json_extract(value, '%s') from $table$ where first_key=? and second_key=?;", path)
	} else {
		rawQuery = fmt.Sprintf(
			"select json_unquote(json_extract(value, '%s')) from $table$ where first_key=? and second_key=?;", path)
	}

	o, err := l.Client().QueryFirst(rawQuery, StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (l *JSONMappingList) Close() error {
	return l.DB.sqlDb.Close()
}
