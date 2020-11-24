package bome

import (
	"database/sql"
	"log"
)

// DoubleMap is a convenience for double mapping persistent store
type DoubleMap interface {
	Contains(firstKey, secondKey string) (bool, error)
	Save(m *DoubleMapEntry) error
	Get(firstKey, secondKey string) (string, error)
	RangeMatchingFirstKey(key string, offset, count int) ([]*MapEntry, error)
	RangeMatchingSecondKey(key string, offset, count int) ([]*MapEntry, error)
	Range(offset, count int) ([]*DoubleMapEntry, error)
	GetForFirst(firstKey string) (Cursor, error)
	GetForSecond(secondKey string) (Cursor, error)
	GetAll() (Cursor, error)
	Delete(firstKey, secondKey string) error
	DeleteAllMatchingFirstKey(firstKey string) error
	DeleteAllMatchingSecondKey(secondKey string) error
	Clear() error
	Close() error
}

type doubleMap struct {
	*Bome
}

func (s *doubleMap) BeginTransaction() (DoubleMapTransaction, error) {
	tx, err := s.Bome.BeginTx()
	if err != nil {
		return nil, err
	}

	return &txDoubleMap{
		doubleMap: s,
		tx:        tx,
	}, nil
}

func (s *doubleMap) Client() Client {
	return s.Bome
}

func (s *doubleMap) Contains(firstKey, secondKey string) (bool, error) {
	o, err := s.Client().SQLQueryFirst("select 1 from $prefix$ where first_key=? and second_key=?;", BoolScanner, firstKey, secondKey)
	return o.(bool), err
}

func (s *doubleMap) Save(m *DoubleMapEntry) error {
	if s.Client().SQLExec("insert into $prefix$ values (?, ?, ?);", m.FirstKey, m.SecondKey, m.Value) != nil {
		return s.Client().SQLExec("update $prefix$ set value=? where first_key=? and second_key=?;", m.Value, m.FirstKey, m.SecondKey)
	}
	return nil
}

func (s *doubleMap) Get(firstKey, secondKey string) (string, error) {
	o, err := s.Client().SQLQueryFirst("select value from $prefix$ where first_key=? and second_key=?;", StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (s *doubleMap) RangeMatchingFirstKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := s.Client().SQLQuery("select second_key, value from $prefix$ where first_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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

func (s *doubleMap) RangeMatchingSecondKey(key string, offset, count int) ([]*MapEntry, error) {
	c, err := s.Client().SQLQuery("select first_key, value from $prefix$ where second_key=? limit ?, ?;", MapEntryScanner, key, offset, count)
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

func (s *doubleMap) Range(offset, count int) ([]*DoubleMapEntry, error) {
	c, err := s.Client().SQLQuery("select * from $prefix$ limit ?, ?;", DoubleMapEntryScanner, offset, count)
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

func (s *doubleMap) GetForFirst(firstKey string) (Cursor, error) {
	return s.Client().SQLQuery("select second_key, value from $prefix$ where first_key=?;", MapEntryScanner, firstKey)
}

func (s *doubleMap) GetForSecond(secondKey string) (Cursor, error) {
	return s.Client().SQLQuery("select first_key, value from $prefix$ where second_key=?;", MapEntryScanner, secondKey)
}

func (s *doubleMap) GetAll() (Cursor, error) {
	return s.Client().SQLQuery("select * from $prefix$;", DoubleMapEntryScanner)
}

func (s *doubleMap) Delete(firstKey, secondKey string) error {
	return s.Client().SQLExec("delete from $prefix$ where first_key=? and second_key=?;", firstKey, secondKey)
}

func (s *doubleMap) DeleteAllMatchingFirstKey(firstKey string) error {
	return s.Client().SQLExec("delete from $prefix$ where first_key=?;", firstKey)
}

func (s *doubleMap) DeleteAllMatchingSecondKey(secondKey string) error {
	return s.Client().SQLExec("delete from $prefix$ where second_key=?;", secondKey)
}

func (s *doubleMap) Clear() error {
	return s.Client().SQLExec("delete from $prefix$;")
}

func (s *doubleMap) Close() error {
	return s.Bome.sqlDb.Close()
}

// NewDoubleMap creates MySQL wrapped DoubleMap
func NewDoubleMap(db *sql.DB, dialect string, tableName string) (DoubleMap, error) {
	d := new(doubleMap)
	var err error

	if dialect == SQLite3 {
		d.Bome, err = NewLite(db)

	} else if dialect == MySQL {
		d.Bome, err = New(db)

	} else {
		return nil, DialectNotSupported
	}

	d.SetTablePrefix(tableName).
		AddTableDefinition(
			"create table if not exists $prefix$ (first_key varchar(255) not null, second_key varchar(255) not null, value longtext not null);",
		)

	err = d.Init()
	if err != nil {
		return nil, err
	}

	err = d.AddUniqueIndex(Index{Name: "unique_keys", Table: "$prefix$", Fields: []string{"first_key", "second_key"}}, false)
	return d, err
}
