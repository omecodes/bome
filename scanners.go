package bome

const (
	//IntScanner is the key for integer scanner
	IntScanner = "scanInt"

	//BoolScanner is the key for boolean scanner
	BoolScanner = "scanBool"

	//StringScanner is the key for string scanner
	StringScanner = "scanString"

	//ListEntryScanner is the key for  list entry scanner
	ListEntryScanner = "scanListEntry"

	//MapEntrySCanner is the key for map entry scanner
	MapEntrySCanner = "scanMapEntry"

	//DoubleMapEntryScanner is the key for double map scanner
	DoubleMapEntryScanner = "scanDoubleMapEntry"
)

var defaultScanners = map[string]Scanner{
	IntScanner: NewScannerFunc(func(row Row) (interface{}, error) {
		var v int64
		return v, row.Scan(&v)
	}),
	BoolScanner: NewScannerFunc(func(row Row) (interface{}, error) {
		var v int
		return v == 1, row.Scan(&v)
	}),
	StringScanner: NewScannerFunc(func(row Row) (interface{}, error) {
		var v string
		return v, row.Scan(&v)
	}),
	ListEntryScanner:      NewScannerFunc(scanListEntry),
	MapEntrySCanner:       NewScannerFunc(scanMapEntry),
	DoubleMapEntryScanner: NewScannerFunc(scanDoubleMapEntry),
}
