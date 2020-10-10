package bome

// Map is a convenience for persistent string to string dict
type Map interface {
	Save(entry *MapEntry) error
	Get(key string) (string, error)
	Contains(key string) (bool, error)
	Delete(key string) error
	List() (Cursor, error)
	Clear() error
	Close() error
}

type sqlObjects struct {
	*DB
}

func (s *sqlObjects) Save(entry *MapEntry) error {
	err := s.Exec("insert", entry.Key, entry.Value).Error
	if err != nil {
		err = s.Exec("update", entry.Value, entry.Key).Error
	}
	return err
}

func (s *sqlObjects) Get(key string) (string, error) {
	o, err := s.QueryFirst("select", MapEntrySCanner, key)
	if err != nil {
		return "", err
	}
	entry := o.(*MapEntry)
	return entry.Value, nil
}

func (s *sqlObjects) Contains(key string) (bool, error) {
	res, err := s.QueryFirst("contains", BoolScanner, key)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (s *sqlObjects) Delete(key string) error {
	return s.Exec("delete", key).Error
}

func (s *sqlObjects) List() (Cursor, error) {
	return s.Query("select_all", MapEntrySCanner)
}

func (s *sqlObjects) Clear() error {
	return s.Exec("clear").Error
}

func (s *sqlObjects) Close() error {
	return s.DB.sqlDb.Close()
}

// NewSQLMap creates an sql map which entries are store in a table that have name created with concataning name and '_map'
func NewSQLMap(dsn string, name string) (Map, error) {
	d := new(sqlObjects)
	db, err := Create(dsn)
	if err != nil {
		return nil, nil
	}
	d.DB = db

	d.SetTablePrefix(name).
		AddTableDefinition("create table if not exists $prefix$_map (name varchar(255) not null primary key, val longblob not null);").
		AddStatement("insert", "insert into $prefix$_mapping values (?, ?);").
		AddStatement("update", "update $prefix$_mapping set val=? where name=?;").
		AddStatement("select", "select * from $prefix$_mapping where name=?;").
		AddStatement("select_all", "select * from $prefix$_mapping;").
		AddStatement("contains", "select 1 from $prefix$_mapping where name=?;").
		AddStatement("delete", "delete from $prefix$_mapping where name=?;").
		AddStatement("clear", "delete from $prefix$_mapping;")
	return d, d.Init()
}
