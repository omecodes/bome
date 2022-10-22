package bome

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type List struct {
	*JsonValueHolder
	*DB
	tx        *TX
	tableName string
	dialect   string
}

func (l *List) Table() string {
	return l.tableName
}

func (l *List) Keys() []string {
	return []string{
		"ind",
	}
}

func (l *List) Transaction(ctx context.Context) (context.Context, *List, error) {
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
		return newCtx, &List{
			JsonValueHolder: &JsonValueHolder{
				dialect: l.dialect,
				tx:      tx,
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
	return newCtx, &List{
		JsonValueHolder: &JsonValueHolder{
			dialect: l.dialect,
			tx:      tx,
		},
		tableName: l.tableName,
		tx:        tx,
		dialect:   l.dialect,
	}, nil
}

func (l *List) Client() Client {
	if l.tx != nil {
		return l.tx
	}
	return l.DB
}

func (l *List) EditAt(index int64, path string, ex Expression) error {
	rawQuery := fmt.Sprintf(
		"update $table$ set value=json_set(value, '%s', %s) where ind=?;", path, ex.eval())
	return l.Client().Exec(rawQuery, index).Error
}

func (l *List) ExtractAt(index int64, path string) (string, error) {
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

func (l *List) SaveAt(index int64, o interface{}, opts SaveOptions) error {
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}

	err = l.Client().Exec("insert into $table$ values (?, ?);", index, string(data)).Error
	if err != nil && isPrimaryKeyConstraintError(err) && opts.UpdateExisting {
		return l.Client().Exec("update $table$ set value=? where ind=?;", string(data), index).Error
	}
	return err
}

func (l *List) Save(value string) error {
	return l.Client().Exec("insert into $table$ (value) values (?);", value).Error
}

func (l *List) Read(index int64, o interface{}) error {
	if o == nil {
		o = reflect.New(reflect.TypeOf(o))
	}

	value, err := l.Client().QueryFirst("select value from $table$ where ind=?;", StringScanner, index)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(value.(string)), o)
}

func (l *List) MinIndex() (int64, error) {
	res, err := l.Client().QueryFirst("select min(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *List) MaxIndex() (int64, error) {
	res, err := l.Client().QueryFirst("select max(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *List) Count() (int64, error) {
	res, err := l.Client().QueryFirst("select count(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *List) Size(index int64) (int64, error) {
	o, err := l.Client().QueryFirst("select coalesce(length(value)), 0) from $table$ where ind=?;", IntScanner, index)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (l *List) TotalSize() (int64, error) {
	o, err := l.Client().QueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (l *List) ReadNext(index int64, o interface{}) error {
	if o == nil {
		o = reflect.New(reflect.TypeOf(o))
	}

	value, err := l.Client().QueryFirst("select * from $table$ where ind>? order by ind;", ListEntryScanner, index)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(value.(string)), o)
}

func (l *List) RangeFrom(index int64, offset, count int) (Cursor, error) {
	return l.Client().Query("select value from $table$ where ind>? order by ind limit ?, ?;", StringScanner, index, offset, count)
}

func (l *List) Range(offset, count int) (Cursor, error) {
	return l.Client().Query("select * from $table$ order by ind limit ?, ?;", ListEntryScanner, offset, count)
}

func (l *List) IndexInRange(after, before int64) (Cursor, int64, error) {
	var (
		total int64
		c     Cursor
	)

	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind > ? and ind < ?;", IntScanner, after, before)
	if err != nil {
		return nil, 0, err
	}
	total = o.(int64)

	c, err = l.Client().Query("select * from $table$ where ind > ? and ind < ?;", ListEntryScanner, after, before)
	if err != nil {
		return nil, 0, err
	}

	return c, total, nil
}

func (l *List) IndexBefore(index int64) (Cursor, int64, error) {
	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind < ?;", IntScanner, index)
	if err != nil {
		return nil, 0, err
	}
	total := o.(int64)
	c, err := l.Client().Query("select * from $table$ where ind<? order by ind;", ListEntryScanner, index)
	return c, total, err
}

func (l *List) IndexAfter(index int64) (Cursor, int64, error) {
	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind>?;", IntScanner, index)
	if err != nil {
		return nil, 0, err
	}
	total := o.(int64)
	c, err := l.Client().Query("select * from $table$ where ind>? order by ind;", ListEntryScanner, index)
	return c, total, err
}

func (l *List) Delete(index int64) error {
	return l.Client().Exec("delete from $table$ where ind=?;", index).Error
}

func (l *List) Clear() error {
	return l.Client().Exec("delete from $table$;").Error
}

func (l *List) Close() error {
	return l.DB.sqlDb.Close()
}

func (l *List) Commit() error {
	if l.tx != nil {
		return l.tx.Commit()
	}
	return nil
}

func (l *List) Rollback() error {
	if l.tx != nil {
		return l.tx.Rollback()
	}
	return nil
}
