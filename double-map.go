package bome

import (
	"context"
	"log"
)

// DoubleMap is a double key value map manager an SQL table
type DoubleMap struct {
	tableName string
	dialect   string
	tx        *TX
	*DB
}

func (s *DoubleMap) Table() string {
	return s.tableName
}

func (s *DoubleMap) Keys() []string {
	return []string{
		"first_key", "second_key",
	}
}

func (s *DoubleMap) Transaction(ctx context.Context) (context.Context, *DoubleMap, error) {
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
		return newCtx, &DoubleMap{
			tableName: s.tableName,
			tx:        tx,
			dialect:   s.dialect,
		}, nil
	}

	return ctx, &DoubleMap{
		tableName: s.tableName,
		tx:        tx,
		dialect:   s.dialect,
	}, nil
}

func (s *DoubleMap) SwitchToTransactionMode(tx *TX) *DoubleMap {
	return &DoubleMap{
		tableName: s.tableName,
		tx:        tx,
		dialect:   s.dialect,
	}
}

func (s *DoubleMap) Client() Client {
	if s.tx != nil {
		return s.tx
	}
	return s.DB
}

func (s *DoubleMap) Contains(firstKey, secondKey string) (bool, error) {
	o, err := s.Client().QueryFirst("select 1 from $table$ where first_key=? and second_key=?;", BoolScanner, firstKey, secondKey)
	return o.(bool), err
}

func (s *DoubleMap) Count() (int, error) {
	o, err := s.Client().QueryFirst("select count(*) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *DoubleMap) CountForFirstKey(key string) (int, error) {
	o, err := s.Client().QueryFirst("select count(*) from $table$ where first_key=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *DoubleMap) CountForSecondKey(key string) (int, error) {
	o, err := s.Client().QueryFirst("select count(*) from $table$ where second_key=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *DoubleMap) Size(firstKey string, secondKey string) (int64, error) {
	o, err := s.Client().QueryFirst("select coalesce(length(value), 0) from $table$ where first_key=? and second_key=?;", IntScanner, firstKey, secondKey)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *DoubleMap) TotalSize() (int64, error) {
	o, err := s.Client().QueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *DoubleMap) Save(entry *DoubleMapEntry) error {
	return s.Client().Exec("insert into $table$ values (?, ?, ?);", entry.FirstKey, entry.SecondKey, entry.Value).Error
}

func (s *DoubleMap) Update(entry *DoubleMapEntry) error {
	return s.Client().Exec("update $table$ set value=? where first_key=? and second_key=?;", entry.Value, entry.FirstKey, entry.SecondKey).Error
}

func (s *DoubleMap) Upsert(entry *DoubleMapEntry) error {
	err := s.Save(entry)
	if err != nil && IsPrimaryKeyConstraintError(err) {
		err = s.Update(entry)
	}
	return err
}

func (s *DoubleMap) Get(firstKey, secondKey string) (string, error) {
	o, err := s.Client().QueryFirst("select value from $table$ where first_key=? and second_key=?;", StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (s *DoubleMap) RangeMatchingFirstKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := s.Client().Query("select second_key, value from $table$ where first_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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

func (s *DoubleMap) RangeMatchingSecondKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := s.Client().Query("select first_key, value from $table$ where second_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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

func (s *DoubleMap) Range(offset, count int) ([]*DoubleMapEntry, error) {
	c, err := s.Client().Query("select * from $table$ limit ?, ?;", DoubleMapEntryScanner, offset, count)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := c.Close(); err != nil {
			log.Println(err)
		}
	}()
	var entries []*DoubleMapEntry

	for c.HasNext() {
		o, err := c.Next()
		if err != nil {
			return nil, err
		}
		entries = append(entries, o.(*DoubleMapEntry))
	}
	return entries, nil
}

func (s *DoubleMap) GetForFirst(firstKey string) (Cursor, error) {
	return s.Client().Query("select second_key, value from $table$ where first_key=?;", MapEntryScanner, firstKey)
}

func (s *DoubleMap) GetForSecond(secondKey string) (Cursor, error) {
	return s.Client().Query("select first_key, value from $table$ where second_key=?;", MapEntryScanner, secondKey)
}

func (s *DoubleMap) GetAll() (Cursor, error) {
	return s.Client().Query("select * from $table$;", DoubleMapEntryScanner)
}

func (s *DoubleMap) Delete(firstKey, secondKey string) error {
	return s.Client().Exec("delete from $table$ where first_key=? and second_key=?;", firstKey, secondKey).Error
}

func (s *DoubleMap) DeleteAllMatchingFirstKey(firstKey string) error {
	return s.Client().Exec("delete from $table$ where first_key=?;", firstKey).Error
}

func (s *DoubleMap) DeleteAllMatchingSecondKey(secondKey string) error {
	return s.Client().Exec("delete from $table$ where second_key=?;", secondKey).Error
}

func (s *DoubleMap) Clear() error {
	return s.Client().Exec("delete from $table$;").Error
}

func (s *DoubleMap) Close() error {
	return s.DB.sqlDb.Close()
}

func (s *DoubleMap) Commit() error {
	if s.tx != nil {
		return s.tx.Commit()
	}
	return nil
}

func (s *DoubleMap) Rollback() error {
	if s.tx != nil {
		return s.tx.Rollback()
	}
	return nil
}
