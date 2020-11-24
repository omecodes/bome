package bome

import (
	"database/sql"
	"log"
)

// Map is a convenience for persistent string to string dict
type Map interface {
	Save(entry *MapEntry) error
	Get(key string) (string, error)
	Contains(key string) (bool, error)
	Range(offset, count int) ([]*MapEntry, error)
	Delete(key string) error
	List() (Cursor, error)
	Clear() error
	Close() error
}

type dict struct {
	*Bome
}

func (d *dict) Save(entry *MapEntry) error {
	err := d.RawExec("insert into $prefix$ values (?, ?);", entry.Key, entry.Value).Error
	if err != nil {
		err = d.RawExec("update $prefix$ set value=? where name=?;", entry.Value, entry.Key).Error
	}
	return err
}

func (d *dict) Get(key string) (string, error) {
	o, err := d.RawQueryFirst("select value from $prefix$ where name=?;", StringScanner, key)
	if err != nil {
		return "", err
	}
	return o.(string), nil
}

func (d *dict) Contains(key string) (bool, error) {
	res, err := d.RawQueryFirst("select 1 from $prefix$ where name=?;", BoolScanner, key)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return res.(bool), nil
}

func (d *dict) Range(offset, count int) ([]*MapEntry, error) {
	c, err := d.RawQuery("select * from $prefix$ limit ?, ?;", MapEntryScanner, offset, count)
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

func (d *dict) Delete(key string) error {
	return d.RawExec("delete from $prefix$ where name=?;", key).Error
}

func (d *dict) List() (Cursor, error) {
	return d.RawQuery("select * from $prefix$;", MapEntryScanner)
}

func (d *dict) Clear() error {
	return d.RawExec("delete from $prefix$;").Error
}

func (d *dict) Close() error {
	return d.Bome.sqlDb.Close()
}

// NewMap creates MySQL wrapped map
func NewMap(db *sql.DB, dialect string, tableName string) (Map, error) {
	d := new(dict)
	var err error

	if dialect == SQLite3 {
		d.Bome, err = NewLite(db)
	} else if dialect == MySQL {
		d.Bome, err = New(db)
	} else {
		return nil, DialectNotSupported
	}

	if err != nil {
		return nil, err
	}

	d.SetTablePrefix(tableName).
		AddTableDefinition("create table if not exists $prefix$ (name varchar(255) not null primary key, value longtext not null);")
	return d, d.Init()
}
