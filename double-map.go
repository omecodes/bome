package bome

import "database/sql"

// DoubleMap is a convenience for double mapping persistent store
type DoubleMap interface {
	Contains(firstKey, secondKey string) (bool, error)
	Save(m *DoubleMapEntry) error
	Get(firstKey, secondKey string) (string, error)
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

func (s *doubleMap) Contains(firstKey, secondKey string) (bool, error) {
	o, err := s.QueryFirst("contains", BoolScanner, firstKey, secondKey)
	return o.(bool), err
}

func (s *doubleMap) Save(m *DoubleMapEntry) error {
	if s.Exec("insert", m.FirstKey, m.SecondKey, m.Value).Error != nil {
		return s.Exec("update", m.Value, m.FirstKey, m.SecondKey).Error
	}
	return nil
}

func (s *doubleMap) Get(firstKey, secondKey string) (string, error) {
	o, err := s.QueryFirst("select", StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (s *doubleMap) GetForFirst(firstKey string) (Cursor, error) {
	return s.Query("select_by_first_key", MapEntryScanner, firstKey)
}

func (s *doubleMap) GetForSecond(secondKey string) (Cursor, error) {
	return s.Query("select_by_second_key", MapEntryScanner, secondKey)
}

func (s *doubleMap) GetAll() (Cursor, error) {
	return s.Query("select_all", DoubleMapEntryScanner)
}

func (s *doubleMap) Delete(firstKey, secondKey string) error {
	return s.Exec("delete", firstKey, secondKey).Error
}

func (s *doubleMap) DeleteAllMatchingFirstKey(firstKey string) error {
	return s.Exec("delete_by_first_key", firstKey).Error
}

func (s *doubleMap) DeleteAllMatchingSecondKey(secondKey string) error {
	return s.Exec("delete_by_second_key", secondKey).Error
}

func (s *doubleMap) Clear() error {
	return s.Exec("clear").Error
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
		AddTableDefinition("create table if not exists $prefix$ (first_key varchar(255) not null, second_key varchar(255) not null, value longtext not null);").
		AddStatement("contains", "select 1 from $prefix$ where first_key=? and second_key=?;").
		AddStatement("insert", "insert into $prefix$ values (?, ?, ?);").
		AddStatement("update", "update $prefix$ set value=? where first_key=? and second_key=?;").
		AddStatement("select", "select value from $prefix$ where first_key=? and second_key=?;").
		AddStatement("select_by_first_key", "select second_key, value from $prefix$ where first_key=?;").
		AddStatement("select_by_second_key", "select first_key, value from $prefix$ where second_key=?;").
		AddStatement("select_all", "select * from $prefix$;").
		AddStatement("delete", "delete from $prefix$ where first_key=? and second_key=?;").
		AddStatement("delete_by_first_key", "delete from $prefix$ where first_key=?;").
		AddStatement("delete_by_second_key", "delete from $prefix$ where second_key=?;").
		AddStatement("clear", "delete from $prefix$;")

	err = d.Init()
	if err != nil {
		return nil, err
	}

	err = d.AddUniqueIndex(Index{Name: "unique_keys", Table: "$prefix$", Fields: []string{"first_key", "second_key"}}, false)
	return d, err
}
