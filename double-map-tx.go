package bome

import "log"

type DoubleMapTx struct {
	tx *TX
}

func (tx *DoubleMapTx) Client() Client {
	return tx.tx
}

func (tx *DoubleMapTx) Contains(firstKey, secondKey string) (bool, error) {
	o, err := tx.Client().SQLQueryFirst("select 1 from $table$ where first_key=? and second_key=?;", BoolScanner, firstKey, secondKey)
	return o.(bool), err
}

func (tx *DoubleMapTx) Count() (int, error) {
	o, err := tx.Client().SQLQueryFirst("select count(*) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *DoubleMapTx) CountForFirstKey(key string) (int, error) {
	o, err := tx.Client().SQLQueryFirst("select count(*) from $table$ where first_key=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *DoubleMapTx) CountForSecondKey(key string) (int, error) {
	o, err := tx.Client().SQLQueryFirst("select count(*) from $table$ where second_key=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *DoubleMapTx) Size(firstKey string, secondKey string) (int, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(length(value), 0) from $table$ where first_key=? and second_key=?;", IntScanner, firstKey, secondKey)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (tx *DoubleMapTx) TotalSize() (int64, error) {
	o, err := tx.Client().SQLQueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (tx *DoubleMapTx) Save(m *DoubleMapEntry) error {
	if tx.Client().SQLExec("insert into $table$ values (?, ?, ?);", m.FirstKey, m.SecondKey, m.Value) != nil {
		return tx.Client().SQLExec("update $table$ set value=? where first_key=? and second_key=?;", m.Value, m.FirstKey, m.SecondKey)
	}
	return nil
}

func (tx *DoubleMapTx) Get(firstKey, secondKey string) (string, error) {
	o, err := tx.Client().SQLQueryFirst("select value from $table$ where first_key=? and second_key=?;", StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (tx *DoubleMapTx) RangeMatchingFirstKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := tx.Client().SQLQuery("select second_key, value from $table$ where first_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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

func (tx *DoubleMapTx) RangeMatchingSecondKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := tx.Client().SQLQuery("select first_key, value from $table$ where second_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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

func (tx *DoubleMapTx) Range(offset, count int) ([]*DoubleMapEntry, error) {
	c, err := tx.Client().SQLQuery("select * from $table$ limit ?, ?;", DoubleMapEntryScanner, offset, count)
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

func (tx *DoubleMapTx) GetForFirst(firstKey string) (Cursor, error) {
	return tx.Client().SQLQuery("select second_key, value from $table$ where first_key=?;", MapEntryScanner, firstKey)
}

func (tx *DoubleMapTx) GetForSecond(secondKey string) (Cursor, error) {
	return tx.Client().SQLQuery("select first_key, value from $table$ where second_key=?;", MapEntryScanner, secondKey)
}

func (tx *DoubleMapTx) GetAll() (Cursor, error) {
	return tx.Client().SQLQuery("select * from $table$;", DoubleMapEntryScanner)
}

func (tx *DoubleMapTx) Delete(firstKey, secondKey string) error {
	return tx.Client().SQLExec("delete from $table$ where first_key=? and second_key=?;", firstKey, secondKey)
}

func (tx *DoubleMapTx) DeleteAllMatchingFirstKey(firstKey string) error {
	return tx.Client().SQLExec("delete from $table$ where first_key=?;", firstKey)
}

func (tx *DoubleMapTx) DeleteAllMatchingSecondKey(secondKey string) error {
	return tx.Client().SQLExec("delete from $table$ where second_key=?;", secondKey)
}

func (tx *DoubleMapTx) Clear() error {
	return tx.Client().SQLExec("delete from $table$;")
}

func (tx *DoubleMapTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *DoubleMapTx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *DoubleMapTx) TX() *TX {
	return tx.tx
}
