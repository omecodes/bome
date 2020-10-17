package bome

import "database/sql"

// DoubleMap is a convenience for double mapping persistent store
type DoubleMap interface {
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

type sqlPairMap struct {
	*Bome
}

func (s *sqlPairMap) Save(m *DoubleMapEntry) error {
	if s.Exec("insert", m.FirstKey, m.SecondKey, m.Value).Error != nil {
		return s.Exec("update", m.Value, m.FirstKey, m.SecondKey).Error
	}
	return nil
}

func (s *sqlPairMap) Get(firstKey, secondKey string) (string, error) {
	o, err := s.QueryFirst("select", StringScanner, firstKey, secondKey)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (s *sqlPairMap) GetForFirst(firstKey string) (Cursor, error) {
	return s.Query("select", MapEntrySCanner, firstKey)
}

func (s *sqlPairMap) GetForSecond(secondKey string) (Cursor, error) {
	return s.Query("select_by_second_key", MapEntrySCanner, secondKey)
}

func (s *sqlPairMap) GetAll() (Cursor, error) {
	return s.Query("select_all", DoubleMapEntryScanner)
}

func (s *sqlPairMap) Delete(firstKey, secondKey string) error {
	return s.Exec("delete", firstKey, secondKey).Error
}

func (s *sqlPairMap) DeleteAllMatchingFirstKey(firstKey string) error {
	return s.Exec("delete_by_first_key", firstKey).Error
}

func (s *sqlPairMap) DeleteAllMatchingSecondKey(secondKey string) error {
	return s.Exec("delete_by_first_key", secondKey).Error
}

func (s *sqlPairMap) Clear() error {
	return s.Exec("clear").Error
}

func (s *sqlPairMap) Close() error {
	return s.Bome.sqlDb.Close()
}

// NewSQLDoubleMap creates an sql double map
func NewSQLDoubleMap(dsn string, name string) (DoubleMap, error) {
	d := new(sqlPairMap)
	db, err := Open(dsn)
	if err != nil {
		return nil, err
	}

	d.Bome = db
	d.SetTablePrefix(name).
		AddTableDefinition("create table if not exists $prefix$_map (first_key varchar(255) not null, second_key varchar(255) not null, val longblob not null);").
		AddStatement("insert", "insert into $prefix$_mapping values (?, ?, ?);").
		AddStatement("update", "update $prefix$_mapping set val=? where first_key=? and second_key=?;").
		AddStatement("select", "select value from $prefix$_mapping where first_key=? and second_key=?;").
		AddStatement("select_by_first_key", "select second_key, val from $prefix$_mapping where first_key=?;").
		AddStatement("select_by_second_key", "select first_key, val from $prefix$_mapping where second_key=?;").
		AddStatement("select_all", "select * from $prefix$_mapping;").
		AddStatement("delete", "delete from $prefix$_mapping where first_key=?;").
		AddStatement("delete_by_first_key", "delete from $prefix$_mapping where first_key=?;").
		AddStatement("delete_by_second_key", "delete from $prefix$_mapping where second_key=?;").
		AddStatement("clear", "delete from $prefix$_mapping;")

	err = d.Init()
	if err != nil {
		return nil, err
	}

	err = d.AddUniqueIndex(Index{Name: "unique_keys", Table: "$prefix$_mapping", Fields: []string{"first_key", "second_key"}}, false)
	return d, err
}

// DMapFromSQLDB creates MySQL wrapped DoubleMap
func DMapFromSQLDB(dialect string, db *sql.DB, name string) (DoubleMap, error) {
	d := new(sqlPairMap)
	var err error

	if dialect == SQLite3 {
		d.Bome, err = NewLite(db)
	} else if dialect == MySQL {
		d.Bome, err = New(db)
	} else {
		return nil, DialectNotSupported
	}

	d.SetTablePrefix(name).
		AddTableDefinition("create table if not exists $prefix$_map (first_key varchar(255) not null, second_key varchar(255) not null, value longtext not null);").
		AddStatement("insert", "insert into $prefix$_map values (?, ?, ?);").
		AddStatement("update", "update $prefix$_map set value=? where first_key=? and second_key=?;").
		AddStatement("select", "select value from $prefix$_map where first_key=? and second_key=?;").
		AddStatement("select_by_first_key", "select second_key, value from $prefix$_map where first_key=?;").
		AddStatement("select_by_second_key", "select first_key, value from $prefix$_map where second_key=?;").
		AddStatement("select_all", "select * from $prefix$_map;").
		AddStatement("delete", "delete from $prefix$_map where first_key=?;").
		AddStatement("delete_by_first_key", "delete from $prefix$_map where first_key=?;").
		AddStatement("delete_by_second_key", "delete from $prefix$_map where second_key=?;").
		AddStatement("clear", "delete from $prefix$_map;")

	err = d.Init()
	if err != nil {
		return nil, err
	}

	err = d.AddUniqueIndex(Index{Name: "unique_keys", Table: "$prefix$_map", Fields: []string{"first_key", "second_key"}}, false)
	return d, err
}
