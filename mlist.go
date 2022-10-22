package bome

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"github.com/omecodes/errors"
)

type MList struct {
	tableName string
	dialect   string
	*DB
	*JsonValueHolder
	*MList
	tx *TX
}

func (l *MList) Table() string {
	return l.tableName
}

func (l *MList) Keys() []string {
	return []string{
		"first_key", "second_key",
	}
}

func (l *MList) Transaction(ctx context.Context) (context.Context, *MList, error) {
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
		return newCtx, &MList{
			JsonValueHolder: &JsonValueHolder{
				dialect: l.dialect,
				tx:      tx,
			},
			MList: &MList{
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
	return newCtx, &MList{
		JsonValueHolder: &JsonValueHolder{
			dialect: l.dialect,
			tx:      tx,
		},
		MList: &MList{
			tableName: l.tableName,
			dialect:   l.dialect,
			tx:        tx,
		},
		tableName: l.tableName,
		tx:        tx,
		dialect:   l.dialect,
	}, nil
}

func (l *MList) Commit() error {
	if l.tx != nil {
		return l.tx.Commit()
	}
	return nil
}

func (l *MList) Rollback() error {
	if l.tx != nil {
		return l.tx.Rollback()
	}
	return nil
}

func (l *MList) Client() Client {
	if l.tx != nil {
		return l.tx
	}
	return l.DB
}

func (l *MList) Write(index int64, key string, o interface{}) error {
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}

	return l.Save(&PairListEntry{
		Index: index,
		Key:   key,
		Value: string(data),
	})
}

func (l *MList) Read(key string, o interface{}) error {
	if o == nil {
		o = reflect.New(reflect.TypeOf(o))
	}
	entry, err := l.Get(key)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(entry.Value), o)
}

func (l *MList) EditAt(key string, path string, ex Expression) error {
	ex.setDialect(l.dialect)
	rawQuery := fmt.Sprintf("update $table$ set value=json_set(value, '%s', \"%s\") where name=?;",
		normalizedJsonPath(path),
		ex.eval(),
	)
	return l.Client().Exec(rawQuery, key).Error
}

func (l *MList) ExtractAt(key string, path string) (string, error) {
	var rawQuery string

	if l.dialect == SQLite3 {
		rawQuery = fmt.Sprintf(
			"select json_extract(value, '%s') from $table$ where name=?;", path)
	} else {
		rawQuery = fmt.Sprintf(
			"select json_unquote(json_extract(value, '%s')) from $table$ where name=?;", path)
	}

	o, err := l.Client().QueryFirst(rawQuery, StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (l *MList) Close() error {
	return l.DB.sqlDb.Close()
}

func (l *MList) Save(entry *PairListEntry) error {
	return l.Client().Exec("insert into $table$ values (?, ?, ?);", entry.Index, entry.Key, entry.Value).Error
}

func (l *MList) Update(key string, value string) error {
	return l.Client().Exec("update $table$ set value=? where name=?;", value, key).Error
}

func (l *MList) Upsert(entry *PairListEntry) error {
	err := l.Save(entry)
	if !isPrimaryKeyConstraintError(err) {
		return err
	}
	return l.Update(entry.Key, entry.Value)
}

func (l *MList) Get(key string) (*ListEntry, error) {
	o, err := l.Client().QueryFirst("select ind, value from $table$ where name=?;", ListEntryScanner, key)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *MList) MinIndex() (int64, error) {
	res, err := l.Client().QueryFirst("select min(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *MList) MaxIndex() (int64, error) {
	res, err := l.Client().QueryFirst("select max(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *MList) Count() (int64, error) {
	res, err := l.Client().QueryFirst("select count(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *MList) SizeAt(index int64) (int64, error) {
	o, err := l.Client().QueryFirst("select coalesce(length(value)), 0) from $table$ where ind=?;", IntScanner, index)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (l *MList) TotalSize() (int64, error) {
	o, err := l.Client().QueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (l *MList) GetNextFromSeq(index int64) (*PairListEntry, error) {
	o, err := l.Client().QueryFirst("select * from $table$ where ind>? order by ind;", PairListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*PairListEntry), nil
}

func (l *MList) RangeFromIndex(index int64, offset, count int) ([]*PairListEntry, error) {
	c, err := l.Client().Query("select * from $table$ where ind>? order by ind limit ?, ?;", PairListEntryScanner, index, offset, count)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()

	var entries []*PairListEntry
	for c.HasNext() {
		o, err := c.Entry()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*PairListEntry))
	}
	return entries, nil
}

func (l *MList) Range(offset, count int) ([]*PairListEntry, error) {
	c, err := l.Client().Query("select * from $table$ order by ind limit ?, ?;", PairListEntryScanner, offset, count)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()

	var entries []*PairListEntry
	for c.HasNext() {
		o, err := c.Entry()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*PairListEntry))
	}
	return entries, nil
}

func (l *MList) IndexInRange(after, before int64) (Cursor, int64, error) {
	var (
		total int64
		c     Cursor
	)

	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind > ? and ind < ?;", IntScanner, after, before)
	if err != nil {
		return nil, 0, err
	}
	total = o.(int64)

	c, err = l.Client().Query("select * from $table$ where ind > ? and ind < ?;", PairListEntryScanner, after, before)
	if err != nil {
		return nil, 0, err
	}

	return c, total, nil
}

func (l *MList) IndexBefore(index int64) (Cursor, int64, error) {
	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind<?;", IntScanner, index)
	if err != nil {
		return nil, 0, err
	}
	total := o.(int64)
	cursor, err := l.Client().Query("select * from $table$ where ind<? order by ind;", PairListEntryScanner, index)
	return cursor, total, err
}

func (l *MList) IndexAfter(index int64) (Cursor, int64, error) {
	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind>?;", IntScanner, index)
	if err != nil {
		return nil, 0, err
	}
	total := o.(int64)
	cursor, err := l.Client().Query("select * from $table$ where ind>? order by ind;", PairListEntryScanner, index)
	return cursor, total, err
}

func (l *MList) DeleteAt(index int64) error {
	return l.Client().Exec("delete from $table$ where ind=?;", index).Error
}

func (l *MList) Size(key string) (int64, error) {
	o, err := l.Client().QueryFirst("select coalesce(length(value), 0) from $table$ where name=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (l *MList) Contains(key string) (bool, error) {
	res, err := l.Client().QueryFirst("select 1 from $table$ where name=?;", BoolScanner, key)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (l *MList) Delete(key string) error {
	return l.Client().Exec("delete from $table$ where name=?;", key).Error
}

func (l *MList) List() (Cursor, error) {
	return l.Client().Query("select * from $table$;", PairListEntryScanner)
}

func (l *MList) Clear() error {
	return l.Client().Exec("delete from $table$;").Error
}
