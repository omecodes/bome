package bome

import (
	"database/sql"
	"log"
)

// DoubleMap is a double key value map manager an SQL table
type DoubleMap struct {
	tableName string
	*Bome
}

func (s *DoubleMap) BeginTransaction() (*DoubleMapTx, error) {
	tx, err := s.Bome.BeginTx()
	if err != nil {
		return nil, err
	}

	return &DoubleMapTx{
		tx: tx,
	}, nil
}

func (s *DoubleMap) ContinueTransaction(tx *TX) *DoubleMapTx {
	return &DoubleMapTx{
		tx: tx.clone(s.Bome),
	}
}

func (s *DoubleMap) Client() Client {
	return s.Bome
}

func (s *DoubleMap) Contains(firstKey, secondKey string) (bool, error) {
	o, err := s.Client().SQLQueryFirst("select 1 from $table$ where first_key=? and second_key=?;", BoolScanner, firstKey, secondKey)
	return o.(bool), err
}

func (s *DoubleMap) Count() (int, error) {
	o, err := s.Client().SQLQueryFirst("select count(*) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *DoubleMap) CountForFirstKey(key string) (int, error) {
	o, err := s.Client().SQLQueryFirst("select count(*) from $table$ where first_key=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *DoubleMap) CountForSecondKey(key string) (int, error) {
	o, err := s.Client().SQLQueryFirst("select count(*) from $table$ where second_key=?;", IntScanner, key)
	if err != nil {
		return 0, err
	}
	return o.(int), nil
}

func (s *DoubleMap) Size(firstKey string, secondKey string) (int64, error) {
	o, err := s.Client().SQLQueryFirst("select coalesce(length(value), 0) from $table$ where first_key=? and second_key=?;", IntScanner, firstKey, secondKey)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *DoubleMap) TotalSize() (int64, error) {
	o, err := s.Client().SQLQueryFirst("select coalesce(sum(length(value)), 0) from $table$;", IntScanner)
	if err != nil {
		return 0, err
	}
	return o.(int64), nil
}

func (s *DoubleMap) Save(m *DoubleMapEntry) error {
	if s.Client().SQLExec("insert into $table$ values (?, ?, ?);", m.FirstKey, m.SecondKey, m.Value) != nil {
		return s.Client().SQLExec("update $table$ set value=? where first_key=? and second_key=?;", m.Value, m.FirstKey, m.SecondKey)
	}
	return nil
}

func (s *DoubleMap) Get(firstKey, secondKey string) (string, error) {
	o, err := s.Client().SQLQueryFirst("select value from $table$ where first_key=? and second_key=?;", StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (s *DoubleMap) RangeMatchingFirstKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := s.Client().SQLQuery("select second_key, value from $table$ where first_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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
	c, err := s.Client().SQLQuery("select first_key, value from $table$ where second_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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
	c, err := s.Client().SQLQuery("select * from $table$ limit ?, ?;", DoubleMapEntryScanner, offset, count)
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
	return s.Client().SQLQuery("select second_key, value from $table$ where first_key=?;", MapEntryScanner, firstKey)
}

func (s *DoubleMap) GetForSecond(secondKey string) (Cursor, error) {
	return s.Client().SQLQuery("select first_key, value from $table$ where second_key=?;", MapEntryScanner, secondKey)
}

func (s *DoubleMap) GetAll() (Cursor, error) {
	return s.Client().SQLQuery("select * from $table$;", DoubleMapEntryScanner)
}

func (s *DoubleMap) Delete(firstKey, secondKey string) error {
	return s.Client().SQLExec("delete from $table$ where first_key=? and second_key=?;", firstKey, secondKey)
}

func (s *DoubleMap) DeleteAllMatchingFirstKey(firstKey string) error {
	return s.Client().SQLExec("delete from $table$ where first_key=?;", firstKey)
}

func (s *DoubleMap) DeleteAllMatchingSecondKey(secondKey string) error {
	return s.Client().SQLExec("delete from $table$ where second_key=?;", secondKey)
}

func (s *DoubleMap) Clear() error {
	return s.Client().SQLExec("delete from $table$;")
}

func (s *DoubleMap) Close() error {
	return s.Bome.sqlDb.Close()
}

// NewDoubleMap creates MySQL wrapped DoubleMap
func NewDoubleMap(db *sql.DB, dialect string, tableName string) (*DoubleMap, error) {
	d := new(DoubleMap)
	d.tableName = tableName
	var err error

	if dialect == SQLite3 {
		d.Bome, err = NewLite(db)

	} else if dialect == MySQL {
		d.Bome, err = New(db)

	} else {
		return nil, DialectNotSupported
	}

	d.SetTableName(tableName).
		AddTableDefinition(
			"create table if not exists $table$ (first_key varchar(255) not null, second_key varchar(255) not null, value longtext not null);",
		)

	err = d.Init()
	if err != nil {
		return nil, err
	}

	err = d.AddUniqueIndex(Index{Name: "unique_keys", Table: "$table$", Fields: []string{"first_key", "second_key"}}, false)
	return d, err
}
