package bome

const (
	//IntScanner is the key for integer scanner
	IntScanner = "scanInt"

	//FloatScanner is the key for float scanner
	FloatScanner = "scanFloat"

	//BoolScanner is the key for boolean scanner
	BoolScanner = "scanBool"

	//StringScanner is the key for string scanner
	StringScanner = "scanString"

	//ListEntryScanner is the key for  list entry scanner
	ListEntryScanner = "scanListEntry"

	//MapEntryScanner is the key for map entry scanner
	MapEntryScanner = "scanMapEntry"

	//DoubleMapEntryScanner is the key for double map scanner
	DoubleMapEntryScanner = "scanDoubleMapEntry"

	//PairListEntryScanner is the key for pairs list scanner
	PairListEntryScanner = "scanPairListEntry"
)

var defaultScanners = map[string]Scanner{
	IntScanner: NewScannerFunc(func(row Row) (interface{}, error) {
		var v int64
		return v, row.Scan(&v)
	}),
	FloatScanner: NewScannerFunc(func(row Row) (interface{}, error) {
		var v float64
		return v, row.Scan(&v)
	}),
	BoolScanner: NewScannerFunc(func(row Row) (interface{}, error) {
		var v int
		err := row.Scan(&v)
		return v == 1, err
	}),
	StringScanner: NewScannerFunc(func(row Row) (interface{}, error) {
		var v string
		return v, row.Scan(&v)
	}),
	ListEntryScanner:      NewScannerFunc(scanListEntry),
	MapEntryScanner:       NewScannerFunc(scanMapEntry),
	DoubleMapEntryScanner: NewScannerFunc(scanDoubleMapEntry),
	PairListEntryScanner:  NewScannerFunc(scanPairListEntry),
}
