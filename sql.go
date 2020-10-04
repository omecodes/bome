package bome

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
)

const (
	MySQLIndexScanner  = "mysql_index_scanner"
	SQLiteIndexScanner = "sqlite_index_scanner"

	VarPrefix        = "$prefix$"
	VarEngine        = "$engine$"
	VarAutoIncrement = "$auto_increment$"
	VarLocate        = "$locate$"

	// ScannerIndex = "scanner_index"
)

type Result struct {
	Error        error
	LastInserted int64
	AffectedRows int64
}

type SQLIndex struct {
	Name   string
	Table  string
	Fields []string
}

type DB struct {
	sqlDb                         *sql.DB
	mux                        *sync.RWMutex
	dialect                    string
	isSQLite                   bool
	compiledStatements         map[string]*sql.Stmt
	vars                       map[string]string
	tableDefs                  []string
	registeredStatements       map[string]string
	registeredSQLiteStatements map[string]string
	registeredMySQLStatements  map[string]string
	migrationScripts           []string
	scanners                   map[string]RowScannerV2
	initDone                   bool
}

func Create(dsn string) (*DB, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "sqlite3" {
		db, err := sql.Open("sqlite3", strings.TrimPrefix(dsn, "sqlite3://"))
		if err != nil {
			return nil, err
		}

		dao := new (DB)
		dao.sqlDb = db
		dao.isSQLite = true
		dao.dialect = "sqlite3"
		dao.SetVariable(VarLocate, "instr")
		dao.SetVariable(VarAutoIncrement, "AUTOINCREMENT")
		dao.SetVariable(VarEngine, "")
		if _, err := dao.sqlDb.Exec("PRAGMA foreign_keys=ON"); err != nil {
			return nil, err
		}
		dao.mux = new(sync.RWMutex)
		return dao, nil

	} else if u.Scheme == "mysql" {
		db, err := sql.Open("sqlite3", strings.TrimPrefix(dsn, "mysql://"))
		if err != nil {
			return nil, err
		}

		dao := new (DB)
		dao.sqlDb = db
		dao.dialect = "mysql"
		dao.SetVariable(VarLocate, "locate")
		dao.SetVariable(VarAutoIncrement, "AUTO_INCREMENT")
		dao.SetVariable(VarEngine, "engine=InnoDB")
		return dao, nil

	} else {
		return nil, DialectNotSupported
	}
}

func (dao *DB) Init() error {
	return dao.init()
}

func (dao *DB) init() error {
	dao.RegisterScanner(MySQLIndexScanner, NewScannerFunc(dao.mysqlIndexScan))
	dao.RegisterScanner(SQLiteIndexScanner, NewScannerFunc(dao.sqliteIndexScan))

	if dao.tableDefs != nil && len(dao.tableDefs) > 0 {
		for _, schema := range dao.tableDefs {
			for name, value := range dao.vars {
				schema = strings.Replace(schema, name, value, -1)
			}
			_, err := dao.sqlDb.Exec(schema)
			if err != nil {
				return err
			}
		}
	}

	var specificStatements map[string]string
	if dao.isSQLite && dao.registeredSQLiteStatements != nil {
		specificStatements = dao.registeredSQLiteStatements
	} else {
		specificStatements = dao.registeredMySQLStatements
	}

	if specificStatements != nil {
		if dao.registeredStatements == nil {
			dao.registeredStatements = map[string]string{}
		}
		for name, stmt := range specificStatements {
			dao.registeredStatements[name] = stmt
		}
	}

	for name, stmt := range dao.registeredStatements {
		for name, value := range dao.vars {
			stmt = strings.Replace(stmt, name, value, -1)
		}
		dao.registeredStatements[name] = stmt
	}

	if dao.registeredStatements != nil && len(dao.registeredStatements) > 0 {
		dao.compiledStatements = map[string]*sql.Stmt{}
		for name, stmt := range dao.registeredStatements {
			/*for name, value := range dao.vars {
				stmt = strings.Replace(stmt, name, value, -1)
			} */
			compiledStmt, err := dao.sqlDb.Prepare(stmt)
			if err != nil {
				return err
			}
			dao.compiledStatements[name] = compiledStmt
		}
	}

	dao.initDone = true
	return nil
}

func (dao *DB) Migrate() error {
	if !dao.initDone {
		return InitError
	}
	for _, ms := range dao.migrationScripts {
		for name, value := range dao.vars {
			ms = strings.Replace(ms, name, value, -1)
		}

		_, err := dao.sqlDb.Exec(ms)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dao *DB) IsSQLite() bool {
	return dao.isSQLite
}

func (dao *DB) SetVariable(name string, value string) *DB {
	if dao.vars == nil {
		dao.vars = map[string]string{}
	}
	dao.vars[name] = value
	return dao
}

func (dao *DB) SetTablePrefix(prefix string) *DB {
	if dao.vars == nil {
		dao.vars = map[string]string{}
	}
	dao.vars[VarPrefix] = prefix
	return dao
}

func (dao *DB) AddMigrationScript(s string) *DB {
	dao.migrationScripts = append(dao.migrationScripts, s)
	return dao
}

func (dao *DB) BeginTx() (*TX, error) {
	tx, err := dao.sqlDb.Begin()
	if err != nil {
		return nil, err
	}

	tr := &TX{}
	tr.Tx = tx
	tr.db = dao
	return tr, nil
}

func (dao *DB) AddUniqueIndex(index SQLIndex, forceUpdate bool) error {
	if !dao.initDone {
		return InitError
	}

	for varName, value := range dao.vars {
		index.Table = strings.Replace(index.Table, varName, value, -1)
	}
	hasIndex, err := dao.TableHasIndex(index)
	if err != nil {
		return err
	}

	var result *Result
	if hasIndex && forceUpdate {
		var dropIndexSQL string
		if dao.dialect == "mysql" {
			dropIndexSQL = fmt.Sprintf("drop index %s on %s", index.Name, index.Table)
		} else {
			dropIndexSQL = fmt.Sprintf("drop index if exists %s", index.Name)
		}

		result = dao.RawExec(dropIndexSQL)
		if result.Error != nil {
			return result.Error
		}
	}

	if !hasIndex || forceUpdate {
		var createIndexSQL string
		if dao.dialect == "mysql" {
			createIndexSQL = fmt.Sprintf("create unique index %s on %s(%s)", index.Name, index.Table, strings.Join(index.Fields, ","))
		} else {
			createIndexSQL = fmt.Sprintf("create unique index if not exists %s on %s(%s)", index.Name, index.Table, strings.Join(index.Fields, ","))
		}

		result = dao.RawExec(createIndexSQL)
		if result.Error != nil {
			return result.Error
		}
	}

	return nil
}

func (dao *DB) AddTableDefinition(schema string) *DB {
	dao.tableDefs = append(dao.tableDefs, schema)
	return dao
}

func (dao *DB) AddStatement(name string, statementStr string) *DB {
	if dao.registeredStatements == nil {
		dao.registeredStatements = map[string]string{}
	}
	dao.registeredStatements[name] = statementStr
	return dao
}

func (dao *DB) AddSQLiteStatement(name string, statementStr string) *DB {
	if dao.registeredSQLiteStatements == nil {
		dao.registeredSQLiteStatements = map[string]string{}
	}
	dao.registeredSQLiteStatements[name] = statementStr
	return dao
}

func (dao *DB) AddMySQLStatement(name string, statementStr string) *DB {
	if dao.registeredMySQLStatements == nil {
		dao.registeredMySQLStatements = map[string]string{}
	}
	dao.registeredMySQLStatements[name] = statementStr
	return dao
}

func (dao *DB) RegisterScanner(name string, scanner RowScannerV2) *DB {
	if dao.scanners == nil {
		dao.scanners = map[string]RowScannerV2{}
	}
	dao.scanners[name] = scanner
	return dao
}

func (dao *DB) TableHasIndex(index SQLIndex) (bool, error) {
	if !dao.initDone {
		return false, InitError
	}

	var (
		scannerName string
		rawQuery    string
	)
	if dao.dialect == "mysql" {
		rawQuery = fmt.Sprintf("SHOW INDEX FROM %s", index.Table)
		scannerName = MySQLIndexScanner
	} else {
		rawQuery = fmt.Sprintf("PRAGMA INDEX_LIST('%s')", index.Table)
		scannerName = SQLiteIndexScanner
	}

	cursor, err := dao.RawQuery(rawQuery, scannerName)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = cursor.Close()
	}()

	for cursor.HasNext() {
		ind, err := cursor.Next()
		if err != nil {
			return false, err
		}

		rowIndex := ind.(SQLIndex)
		if rowIndex.Name == index.Name {
			return true, nil
		}
	}
	return false, nil
}

func (dao *DB) RawQuery(query string, scannerName string, params ...interface{}) (DBCursor, error) {
	for name, value := range dao.vars {
		query = strings.Replace(query, name, value, -1)
	}
	rows, err := dao.sqlDb.Query(query, params...)
	if err != nil {
		return nil, err
	}
	scanner, err := dao.findScanner(scannerName)
	if err != nil {
		return nil, err
	}
	return NewSQLDBCursor(rows, scanner), nil
}

func (dao *DB) RawQueryFirst(query string, scannerName string, params ...interface{}) (interface{}, error) {
	for name, value := range dao.vars {
		query = strings.Replace(query, name, value, -1)
	}

	rows, err := dao.sqlDb.Query(query, params...)
	if err != nil {
		return nil, err
	}
	scanner, err := dao.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := NewSQLDBCursor(rows, scanner)
	defer func() {
		_ = cursor.Close()
	}()

	if !cursor.HasNext() {
		return nil, EntryNotFound
	}
	return cursor.Next()
}

func (dao *DB) RawExec(rawQuery string) *Result {
	dao.wLock()
	defer dao.wUnlock()
	var r sql.Result
	result := &Result{}
	for name, value := range dao.vars {
		rawQuery = strings.Replace(rawQuery, name, value, -1)
	}
	r, result.Error = dao.sqlDb.Exec(rawQuery)
	if result.Error == nil && dao.dialect != "sqlite3" {
		result.LastInserted, _ = r.LastInsertId()
		result.AffectedRows, _ = r.RowsAffected()
	}
	return result
}

func (dao *DB) Query(stmt string, scannerName string, params ...interface{}) (DBCursor, error) {
	dao.rLock()
	defer dao.rUnLock()

	st := dao.getStatement(stmt)
	if st == nil {
		return nil, fmt.Errorf("statement `%s` does not exist", stmt)
	}

	rows, err := st.Query(params...)
	if err != nil {
		return nil, err
	}

	scanner, err := dao.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := NewSQLDBCursor(rows, scanner)
	return cursor, nil
}

func (dao *DB) QueryFirst(stmt string, scannerName string, params ...interface{}) (interface{}, error) {
	dao.rLock()
	defer dao.rUnLock()

	st := dao.getStatement(stmt)
	if st == nil {
		return nil, fmt.Errorf("statement `%s` does not exist", stmt)
	}

	rows, err := st.Query(params...)
	if err != nil {
		return nil, err
	}

	scanner, err := dao.findScanner(scannerName)
	if err != nil {
		return nil, err
	}

	cursor := NewSQLDBCursor(rows, scanner)
	defer func() {
		_ = cursor.Close()
	}()

	if !cursor.HasNext() {
		return nil, EntryNotFound
	}
	return cursor.Next()
}

func (dao *DB) Exec(stmt string, params ...interface{}) *Result {
	dao.wLock()
	defer dao.wUnlock()

	result := &Result{}
	var (
		st *sql.Stmt
		r  sql.Result
	)

	st, result.Error = dao.findCompileStatement(stmt)
	if result.Error != nil {
		return result
	}

	r, result.Error = st.Exec(params...)
	if result.Error == nil {
		result.LastInserted, _ = r.LastInsertId()
		result.AffectedRows, _ = r.RowsAffected()
	}
	return result
}

func (dao *DB) sqliteIndexScan(row Row) (interface{}, error) {
	dao.rLock()
	defer dao.rUnLock()

	var index SQLIndex
	m, err := dao.rowToMap(row.(*sql.Rows))
	if err != nil {
		return nil, err
	}

	var ok bool
	index.Name, ok = m["name"].(string)
	if !ok {
		return nil, IndexNotFound
	}
	return index, nil
}

func (dao *DB) mysqlIndexScan(row Row) (interface{}, error) {
	var index SQLIndex
	m, err := dao.rowToMap(row.(*sql.Rows))
	if err != nil {
		return nil, err
	}

	index.Name = fmt.Sprintf("%s", m["Key_name"])
	if index.Name == "" {
		return nil, IndexNotFound
	}
	index.Table = fmt.Sprintf("%s", m["Table"])
	if index.Table == "" {
		return nil, TableNotFound
	}
	return index, nil
}

func (dao *DB) rowToMap(rows *sql.Rows) (map[string]interface{}, error) {
	cols, _ := rows.Columns()
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := rows.Scan(columnPointers...); err != nil {
		return nil, err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	m := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}
	return m, nil

}

func (dao *DB) findCompileStatement(name string) (*sql.Stmt, error) {
	if dao.compiledStatements == nil {
		return nil, StatementNotFound
	}

	if compiledStmt, found := dao.compiledStatements[name]; found {
		return compiledStmt, nil
	}
	return nil, StatementNotFound
}

func (dao *DB) findScanner(name string) (RowScannerV2, error) {
	scanner, found := dao.scanners[name]
	if !found {
		return nil, ScannerNotFound
	}
	return scanner, nil
}

func (dao *DB) getStatement(name string) *sql.Stmt {
	if dao.compiledStatements == nil {
		return nil
	}
	s, found := dao.compiledStatements[name]
	if !found {
		return nil
	}
	return s
}

func (dao *DB) rLock() {
	if dao.mux != nil {
		dao.mux.RLock()
	}
}

func (dao *DB) wLock() {
	if dao.mux != nil {
		dao.mux.Lock()
	}
}

func (dao *DB) rUnLock() {
	if dao.mux != nil {
		dao.mux.RUnlock()
	}
}

func (dao *DB) wUnlock() {
	if dao.mux != nil {
		dao.mux.Unlock()
	}
}

type DBCursor interface {
	Next() (interface{}, error)
	HasNext() bool
	Close() error
}

type scannerFunc struct {
	f func(rows Row) (interface{}, error)
}

func (sf *scannerFunc) ScanRow(row Row) (interface{}, error) {
	return sf.f(row)
}

func NewScannerFunc(f func(row Row) (interface{}, error)) RowScannerV2 {
	return &scannerFunc{
		f: f,
	}
}

// SQLCursor
type SQLCursor struct {
	err  error
	scan RowScannerV2
	rows *sql.Rows
}

func NewSQLDBCursor(rows *sql.Rows, scanner RowScannerV2) DBCursor {
	return &SQLCursor{
		scan: scanner,
		rows: rows,
	}
}

func (c *SQLCursor) Close() error {
	return c.rows.Close()
}

func (c *SQLCursor) HasNext() bool {
	return c.rows.Next()
}

func (c *SQLCursor) Next() (interface{}, error) {
	return c.scan.ScanRow(c.rows)
}
