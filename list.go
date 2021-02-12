package bome

import (
	"context"
	"log"
)

type List struct {
	*DB
	tx        *TX
	dialect   string
	tableName string
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
		return newCtx, &List{
			dialect:   l.dialect,
			tableName: l.tableName,
			tx:        tx,
		}, nil
	}

	return ctx, &List{
		dialect:   l.dialect,
		tableName: l.tableName,
		tx:        tx,
	}, nil
}

func (l *List) SwitchToTransactionMode(tx *TX) *List {
	return &List{
		tableName: l.tableName,
		tx:        tx,
		dialect:   l.dialect,
	}
}

func (l *List) Client() Client {
	if l.tx != nil {
		return l.tx
	}
	return l.DB
}

func (l *List) Save(entry *ListEntry) error {
	if entry.Index > 0 {
		return l.Client().Exec("insert into $table$ values (?, ?);", entry.Index, entry.Value).Error
	}
	return l.Client().Exec("insert into $table$ (value) values (?);", entry.Value).Error
}

func (l *List) Update(entry *ListEntry) error {
	return l.Client().Exec("update $table$ set value=? where ind=?;", entry.Value, entry.Index).Error
}

func (l *List) Upsert(entry *ListEntry) error {
	err := l.Save(entry)
	if !IsPrimaryKeyConstraintError(err) {
		return err
	}
	return l.Update(entry)
}

func (l *List) Append(entry *ListEntry) error {
	return l.Client().Exec("insert into $table$ (value) values (?);", entry.Value).Error
}

func (l *List) GetAt(index int64) (*ListEntry, error) {
	o, err := l.Client().QueryFirst("select * from $table$ where ind=?;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
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

func (l *List) GetNextFromSeq(index int64) (*ListEntry, error) {
	o, err := l.Client().QueryFirst("select * from $table$ where ind>? order by ind;", ListEntryScanner, index)
	if err != nil {
		return nil, err
	}
	return o.(*ListEntry), nil
}

func (l *List) RangeFromIndex(index int64, offset, count int) ([]*ListEntry, error) {
	c, err := l.Client().Query("select * from $table$ where ind>? order by ind limit ?, ?;", ListEntryScanner, index, offset, count)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()
	var entries []*ListEntry
	for c.HasNext() {
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*ListEntry))
	}
	return entries, nil
}

func (l *List) Range(offset, count int) ([]*ListEntry, error) {
	c, err := l.Client().Query("select * from $table$ order by ind limit ?, ?;", ListEntryScanner, offset, count)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()

	var entries []*ListEntry
	for c.HasNext() {
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*ListEntry))
	}
	return entries, nil
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
	cursor, err := l.Client().Query("select * from $table$ where ind<? order by ind;", ListEntryScanner, index)
	return cursor, total, err
}

func (l *List) IndexAfter(index int64) (Cursor, int64, error) {
	o, err := l.Client().QueryFirst("select count(ind) from $table$ where ind>?;", IntScanner, index)
	if err != nil {
		return nil, 0, err
	}
	total := o.(int64)
	cursor, err := l.Client().Query("select * from $table$ where ind>? order by ind;", ListEntryScanner, index)
	return cursor, total, err
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
