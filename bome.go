package bome

type Cursor interface {
	HasNext() bool
	Next() (interface{}, error)
	Close() error
}

type Row interface {
	Scan(dest ...interface{}) error
}

type RowScannerV2 interface {
	ScanRow(row Row) (interface{}, error)
}
