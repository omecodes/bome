package bome

import (
	"database/sql"
	"strings"
)

type Builder struct {
	tableName   string
	dialect     string
	conn        *sql.DB
	keys        map[string]*ForeignKey
	indexes     map[string]*Index
	finalSchema string
}

func (b *Builder) SetTableName(table string) *Builder {
	b.tableName = escaped(table)
	return b
}

func (b *Builder) SetConn(conn *sql.DB) *Builder {
	b.conn = conn
	return b
}

func (b *Builder) SetDialect(dialect string) *Builder {
	b.dialect = dialect
	return b
}

func (b *Builder) AddForeignKeys(keys ...*ForeignKey) *Builder {
	if b.keys == nil {
		b.keys = map[string]*ForeignKey{}
	}
	for _, k := range keys {
		b.keys[k.Name] = k
	}
	return b
}

func (b *Builder) AddIndexes(indexes ...*Index) *Builder {
	if b.indexes == nil {
		b.indexes = map[string]*Index{}
	}

	for _, ind := range indexes {
		b.indexes[ind.Name] = ind
	}
	return b
}

func (b *Builder) Map(opts ...Option) (*Map, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
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

func (b *Builder) JSONMap(opts ...Option) (*JSONMap, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
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

func (b *Builder) DoubleMap(opts ...Option) (*DoubleMap, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
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

func (b *Builder) JSONDoubleMap(opts ...Option) (*JSONDoubleMap, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
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

func (b *Builder) List(opts ...Option) (*List, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
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

func (b *Builder) JSONList(opts ...Option) (*JSONList, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
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

func (b *Builder) KeyValueList(opts ...Option) (*MappingList, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
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

func (b *Builder) KeyJSONValueList(opts ...Option) (*JSONMappingList, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
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

func (b *Builder) initTable(fields []string, opts ...Option) (*DB, error) {
	if b.dialect != SQLite3 && b.dialect != "" {
		return nil, DialectNotSupported
	}

	var postInitExec []string

	var (
		err error
		db  *DB
	)

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
				postInitExec = append(postInitExec, fk.AlterTableAddQuery())
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

func (b *Builder) GetTableName() string {
	return b.tableName
}

func (b *Builder) GetDialect() string {
	return b.dialect
}
