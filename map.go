package bome

import (
	"context"
	"github.com/omecodes/errors"
	"log"
)

type Map struct {
	tableName string
	tx        *TX
	dialect   string
	*DB
}

func (m *Map) Table() string {
	return m.tableName
}

func (m *Map) Keys() []string {
	return []string{
		"name",
	}
}

func (m *Map) Transaction(ctx context.Context) (context.Context, *Map, error) {
	tx := transaction(ctx)
	if tx == nil {
		if m.tx != nil {
			return contextWithTransaction(ctx, m.tx.New(m.DB)), m, nil
		}

		var err error
		tx, err = m.BeginTx()
		if err != nil {
			return ctx, nil, err
		}

		newCtx := contextWithTransaction(ctx, tx)
		return newCtx, &Map{
			tableName: m.tableName,
			tx:        tx,
			dialect:   m.dialect,
		}, nil
	}

	if m.tx != nil {
		if m.tx.db.sqlDb != tx.db.sqlDb {
			newCtx := ContextWithCommitActions(ctx, tx.Commit)
			newCtx = ContextWithRollbackActions(newCtx, tx.Rollback)
			return contextWithTransaction(newCtx, m.tx.New(m.DB)), m, nil
		}
		return ctx, m, nil
	}

	tx = tx.New(m.DB)
	newCtx := contextWithTransaction(ctx, tx)
	return newCtx, &Map{
		tableName: m.tableName,
		tx:        tx,
		dialect:   m.dialect,
	}, nil
}

func (m *Map) Client() Client {
	if m.tx != nil {
		return m.tx
	}
	return m.DB
}

func (m *Map) Save(entry *MapEntry) error {
	return m.Client().Exec("insert into $table$ values (?, ?);", entry.Key, entry.Value).Error
}

func (m *Map) Update(entry *MapEntry) error {
	return m.Client().Exec("update $table$ set value=? where name=?;", entry.Value, entry.Key).Error
}

func (m *Map) Upsert(entry *MapEntry) error {
	err := m.Save(entry)
	if !isPrimaryKeyConstraintError(err) {
		return err
	}
	return m.Update(entry)
}

func (m *Map) Get(key string) (string, error) {
	o, err := m.Client().QueryFirst("select value from $table$ where name=?;", StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (m *Map) Size(key string) (int64, error) {
	o, err := m.Client().QueryFirst("select coalesce(length(value), 0) from $table$ where name=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (m *Map) TotalSize() (int64, error) {
	o, err := m.Client().QueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (m *Map) Contains(key string) (bool, error) {
	res, err := m.Client().QueryFirst("select 1 from $table$ where name=?;", BoolScanner, key)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (m *Map) Range(offset, count int) ([]*MapEntry, error) {
	c, err := m.Client().Query("select * from $table$ limit ?, ?;", MapEntryScanner, offset, count)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()

	var entries []*MapEntry
	for c.HasNext() {
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*MapEntry))
	}
	return entries, nil
}

func (m *Map) Delete(key string) error {
	return m.Client().Exec("delete from $table$ where name=?;", key).Error
}

func (m *Map) List() (Cursor, error) {
	return m.Client().Query("select * from $table$;", MapEntryScanner)
}

func (m *Map) Clear() error {
	return m.Client().Exec("delete from $table$;").Error
}

func (m *Map) Close() error {
	return m.DB.sqlDb.Close()
}

func (m *Map) Commit() error {
	if m.tx != nil {
		return m.tx.Commit()
	}
	return nil
}

func (m *Map) Rollback() error {
	if m.tx != nil {
		return m.tx.Rollback()
	}
	return nil
}
