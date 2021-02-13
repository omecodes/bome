package bome

import (
	"context"
	"log"
)

type MappingList struct {
	tableName string
	dialect   string
	*DB
	tx *TX
}

func (l *MappingList) Table() string {
	return l.tableName
}

func (l *MappingList) Keys() []string {
	return []string{
		"ind",
	}
}

func (l *MappingList) Transaction(ctx context.Context) (context.Context, *MappingList, error) {
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
		return newCtx, &MappingList{
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
	return newCtx, &MappingList{
		tableName: l.tableName,
		tx:        tx,
		dialect:   l.dialect,
	}, nil
}

func (l *MappingList) Client() Client {
	if l.tx != nil {
		return l.tx
	}
	return l.DB
}

func (l *MappingList) Save(entry *PairListEntry) error {
	if entry.Index == 0 {
		return l.Client().Exec("insert into $table$ (name, value) values (?, ?);", entry.Key, entry.Value).Error
	} else {
		return l.Client().Exec("insert into $table$ values (?, ?, ?);", entry.Index, entry.Key, entry.Value).Error
	}
}

func (l *MappingList) UpdateKey(key string, value string) error {
	return l.Client().Exec("update $table$ set value=? where name=?;", value, key).Error
}

func (l *MappingList) Upsert(entry *PairListEntry) error {
	err := l.Save(entry)
	if !IsPrimaryKeyConstraintError(err) {
		return err
	}
	return l.UpdateKey(entry.Key, entry.Value)
}

func (l *MappingList) Append(entry *PairListEntry) error {
	return l.Client().Exec("insert into $table$ values (?, ?, ?);", entry.Index, entry.Key, entry.Value).Error
}

func (l *MappingList) AppendPair(entry *MapEntry) error {
	return l.Client().Exec("insert into $table$ (name, value) values (?, ?);", entry.Key, entry.Value).Error
}

func (l *MappingList) GetAt(index int64) (*MapEntry, error) {
	o, err := l.Client().QueryFirst("select name, value from $table$ where ind=?;", MapEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*MapEntry), nil
}

func (l *MappingList) Get(key string) (*ListEntry, error) {
	o, err := l.Client().QueryFirst("select ind, value from $table$ where name=?;", ListEntryScanner, key)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *MappingList) MinIndex() (int64, error) {
	res, err := l.Client().QueryFirst("select min(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *MappingList) MaxIndex() (int64, error) {
	res, err := l.Client().QueryFirst("select max(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *MappingList) Count() (int64, error) {
	res, err := l.Client().QueryFirst("select count(ind) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (l *MappingList) SizeAt(index int64) (int64, error) {
	o, err := l.Client().QueryFirst("select coalesce(length(value)), 0) from $table$ where ind=?;", IntScanner, index)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (l *MappingList) TotalSize() (int64, error) {
	o, err := l.Client().QueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (l *MappingList) GetNextFromSeq(index int64) (*PairListEntry, error) {
	o, err := l.Client().QueryFirst("select * from $table$ where ind>? order by ind;", PairListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*PairListEntry), nil
}

func (l *MappingList) RangeFromIndex(index int64, offset, count int) ([]*PairListEntry, error) {
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
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*PairListEntry))
	}
	return entries, nil
}

func (l *MappingList) Range(offset, count int) ([]*PairListEntry, error) {
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
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*PairListEntry))
	}
	return entries, nil
}

func (l *MappingList) IndexInRange(after, before int64) (Cursor, int64, error) {
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

func (l *MappingList) IndexBefore(index int64) (Cursor, int64, error) {
	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind<?;", IntScanner, index)
	if err != nil {
		return nil, 0, err
	}
	total := o.(int64)
	cursor, err := l.Client().Query("select * from $table$ where ind<? order by ind;", PairListEntryScanner, index)
	return cursor, total, err
}

func (l *MappingList) IndexAfter(index int64) (Cursor, int64, error) {
	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind>?;", IntScanner, index)
	if err != nil {
		return nil, 0, err
	}
	total := o.(int64)
	cursor, err := l.Client().Query("select * from $table$ where ind>? order by ind;", PairListEntryScanner, index)
	return cursor, total, err
}

func (l *MappingList) DeleteAt(index int64) error {
	return l.Client().Exec("delete from $table$ where ind=?;", index).Error
}

func (l *MappingList) Size(key string) (int64, error) {
	o, err := l.Client().QueryFirst("select coalesce(length(value), 0) from $table$ where name=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (l *MappingList) Contains(key string) (bool, error) {
	res, err := l.Client().QueryFirst("select 1 from $table$ where name=?;", BoolScanner, key)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (l *MappingList) Delete(key string) error {
	return l.Client().Exec("delete from $table$ where name=?;", key).Error
}

func (l *MappingList) List() (Cursor, error) {
	return l.Client().Query("select * from $table$;", PairListEntryScanner)
}

func (l *MappingList) Clear() error {
	return l.Client().Exec("delete from $table$;").Error
}

func (l *MappingList) Close() error {
	return l.DB.sqlDb.Close()
}

func (l *MappingList) Commit() error {
	if l.tx != nil {
		return l.tx.Commit()
	}
	return nil
}

func (l *MappingList) Rollback() error {
	if l.tx != nil {
		return l.tx.Rollback()
	}
	return nil
}
