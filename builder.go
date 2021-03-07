package bome

import (
	"database/sql"
	"github.com/omecodes/errors"
	"strings"
)

func Build() *builder {
	return &builder{}
}

type builder struct {
	tableName   string
	dialect     string
	conn        *sql.DB
	keys        map[string]*ForeignKey
	indexes     map[string]*Index
	finalSchema string
}

func (b *builder) SetTableName(table string) *builder {
	b.tableName = escaped(table)
	return b
}

func (b *builder) SetConn(conn *sql.DB) *builder {
	b.conn = conn
	return b
}

func (b *builder) SetDialect(dialect string) *builder {
	b.dialect = dialect
	return b
}

func (b *builder) AddForeignKeys(keys ...*ForeignKey) *builder {
	if b.keys == nil {
		b.keys = map[string]*ForeignKey{}
	}
	for _, k := range keys {
		b.keys[k.Name] = k
	}
	return b
}

func (b *builder) AddIndexes(indexes ...*Index) *builder {
	if b.indexes == nil {
		b.indexes = map[string]*Index{}
	}

	for _, ind := range indexes {
		b.indexes[ind.Name] = ind
	}
	return b
}

func (b *builder) Map(opts ...Option) (*Map, error) {
	if b.dialect != SQLite3 && b.dialect != MySQL {
		return nil, errors.Unsupported("sql dialect not supported", errors.Details{Key: "type", Value: "dialect"}, errors.Details{Key: "name", Value: b.dialect})
	}

	fields := []string{
		"name varchar(255) not null primary key",
		"value longtext not null",
	}

	db, err := b.initTable(fields, opts...)
	if err != nil {
		return nil, err
	}

	return &Map{
		tableName: b.tableName,
		DB:        db,
		dialect:   b.dialect,
	}, nil
}

func (b *builder) JSONMap(opts ...Option) (*JSONMap, error) {
	if b.dialect != SQLite3 && b.dialect != MySQL {
		return nil, errors.Unsupported("sql dialect not supported", errors.Details{Key: "type", Value: "dialect"}, errors.Details{Key: "name", Value: b.dialect})
	}

	fields := []string{
		"name varchar(255) not null primary key",
		"value json not null",
	}

	db, err := b.initTable(fields, opts...)
	if err != nil {
		return nil, err
	}

	return &JSONMap{
		JsonValueHolder: &JsonValueHolder{
			DB:      db,
			field:   "value",
			dialect: b.dialect,
		},
		Map: &Map{
			tableName: b.tableName,
			DB:        db,
			dialect:   b.dialect,
		},
		tableName: b.tableName,
		DB:        db,
		dialect:   b.dialect,
	}, nil
}

func (b *builder) DoubleMap(opts ...Option) (*DoubleMap, error) {
	if b.dialect != SQLite3 && b.dialect != MySQL {
		return nil, errors.Unsupported("sql dialect not supported", errors.Details{Key: "type", Value: "dialect"}, errors.Details{Key: "name", Value: b.dialect})
	}

	fields := []string{
		"first_key varchar(255) not null",
		"second_key varchar(255) not null",
		"value longtext not null",
	}

	db, err := b.initTable(fields, opts...)
	if err != nil {
		return nil, err
	}

	err = db.AddUniqueIndex(Index{Name: "unique_keys", Table: "$table$", Fields: []string{"first_key", "second_key"}}, false)
	return &DoubleMap{
		tableName: b.tableName,
		DB:        db,
		dialect:   b.dialect,
	}, err
}

func (b *builder) JSONDoubleMap(opts ...Option) (*JSONDoubleMap, error) {
	if b.dialect != SQLite3 && b.dialect != MySQL {
		return nil, errors.Unsupported("sql dialect not supported", errors.Details{Key: "type", Value: "dialect"}, errors.Details{Key: "name", Value: b.dialect})
	}

	fields := []string{
		"first_key varchar(255) not null",
		"second_key varchar(255) not null",
		"value json not null",
	}

	db, err := b.initTable(fields, opts...)
	if err != nil {
		return nil, err
	}

	err = db.AddUniqueIndex(Index{Name: "unique_keys", Table: "$table$", Fields: []string{"first_key", "second_key"}}, false)
	if err != nil {
		return nil, err
	}

	return &JSONDoubleMap{
		JsonValueHolder: &JsonValueHolder{
			DB:      db,
			field:   "value",
			dialect: b.dialect,
		},
		DoubleMap: &DoubleMap{
			tableName: b.tableName,
			DB:        db,
			dialect:   b.dialect,
		},
		tableName: b.tableName,
		DB:        db,
		dialect:   b.dialect,
	}, nil
}

func (b *builder) List(opts ...Option) (*List, error) {
	if b.dialect != SQLite3 && b.dialect != MySQL {
		return nil, errors.Unsupported("sql dialect not supported", errors.Details{Key: "type", Value: "dialect"}, errors.Details{Key: "name", Value: b.dialect})
	}

	var fields []string
	if b.dialect == SQLite3 {
		fields = []string{
			"ind integer not null primary key $auto_increment$",
			"value longtext not null",
		}
	} else {
		fields = []string{
			"ind bigint not null primary key $auto_increment$",
			"value longtext not null",
		}
	}

	db, err := b.initTable(fields, opts...)
	if err != nil {
		return nil, err
	}

	return &List{
		tableName: b.tableName,
		DB:        db,
		dialect:   b.dialect,
	}, nil
}

func (b *builder) JSONList(opts ...Option) (*JSONList, error) {
	if b.dialect != SQLite3 && b.dialect != MySQL {
		return nil, errors.Unsupported("sql dialect not supported", errors.Details{Key: "type", Value: "dialect"}, errors.Details{Key: "name", Value: b.dialect})
	}

	var fields []string
	if b.dialect == SQLite3 {
		fields = []string{
			"ind integer not null primary key $auto_increment$",
			"value json not null",
		}
	} else {
		fields = []string{
			"ind bigint not null primary key $auto_increment$",
			"value json not null",
		}
	}

	db, err := b.initTable(fields, opts...)
	if err != nil {
		return nil, err
	}

	return &JSONList{
		JsonValueHolder: &JsonValueHolder{
			DB:      db,
			field:   "value",
			dialect: b.dialect,
		},
		List: &List{
			DB:        db,
			dialect:   b.dialect,
			tableName: b.tableName,
		},
		tableName: b.tableName,
		DB:        db,
		dialect:   b.dialect,
	}, nil
}

func (b *builder) MappingList(opts ...Option) (*MappingList, error) {
	if b.dialect != SQLite3 && b.dialect != MySQL {
		return nil, errors.Unsupported("sql dialect not supported", errors.Details{Key: "type", Value: "dialect"}, errors.Details{Key: "name", Value: b.dialect})
	}

	fields := []string{
		"ind bigint not null $auto_increment$",
		"name varchar(255) not null primary key",
		"value longtext not null",
	}

	db, err := b.initTable(fields, opts...)
	if err != nil {
		return nil, err
	}

	return &MappingList{
		tableName: b.tableName,
		DB:        db,
		dialect:   b.dialect,
	}, nil
}

func (b *builder) JSONMappingList(opts ...Option) (*JSONMappingList, error) {
	if b.dialect != SQLite3 && b.dialect != MySQL {
		return nil, errors.Unsupported("sql dialect not supported", errors.Details{Key: "type", Value: "dialect"}, errors.Details{Key: "name", Value: b.dialect})
	}

	fields := []string{
		"ind bigint not null $auto_increment$",
		"name varchar(255) not null primary key",
		"value json not null",
	}

	db, err := b.initTable(fields, opts...)
	if err != nil {
		return nil, err
	}

	return &JSONMappingList{
		JsonValueHolder: &JsonValueHolder{
			DB:      db,
			field:   "value",
			dialect: b.dialect,
		},
		MappingList: &MappingList{
			DB:        db,
			tableName: b.tableName,
			dialect:   b.dialect,
		},
		tableName: b.tableName,
		DB:        db,
		dialect:   b.dialect,
	}, nil
}

func (b *builder) initTable(fields []string, opts ...Option) (*DB, error) {
	var postInitExec []string

	var (
		err     error
		db      *DB
		options options
	)

	for _, opt := range opts {
		opt(&options)
	}

	for _, fk := range options.foreignKeys {
		b.AddForeignKeys(fk)
	}

	for _, ind := range options.indexes {
		b.AddIndexes(ind)
	}

	if b.dialect == SQLite3 {
		db, err = NewLite(b.conn)
		if err != nil {
			return nil, err
		}

		if len(b.keys) > 0 {
			for _, fk := range b.keys {
				fields = append(fields, fk.InTableDefQuery())
			}
		}

		if len(b.indexes) > 0 {
			for _, ind := range b.indexes {
				postInitExec = append(postInitExec, ind.SQLiteAddQuery())
			}
		}

	} else {
		db, err = New(b.conn)
		if err != nil {
			return nil, err
		}

		if len(b.keys) > 0 {
			for _, fk := range b.keys {
				fields = append(fields, fk.InTableDefQuery())
			}
		}

		if len(b.indexes) > 0 {
			for _, ind := range b.indexes {
				postInitExec = append(postInitExec, ind.MySQLAddQuery())
			}
		}
	}

	header := "create table if not exists $table$"
	tail := "$engine$;"
	body := strings.Join(fields, ",")
	definition := header + "(" + body + ")" + tail

	db.SetTableName(b.tableName)
	db.AddTableDefinition(definition)
	err = db.Init()
	if err != nil {
		return nil, err
	}

	for _, query := range postInitExec {
		err = db.Exec(query).Error
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (b *builder) GetTableName() string {
	return b.tableName
}

func (b *builder) GetDialect() string {
	return b.dialect
}
