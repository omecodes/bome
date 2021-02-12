package bome

// ListEntry is the list entry definition
type ListEntry struct {
	Index int64
	Value string
}

// MapEntry is the map entry definition
type MapEntry struct {
	Key   string
	Value string
}

// DoubleMapEntry is the double map entry definition
type DoubleMapEntry struct {
	FirstKey  string
	SecondKey string
	Value     string
}

// PairListEntry is the pairs list entry definition
type PairListEntry struct {
	Index int64
	Key   string
	Value string
}

func scanListEntry(row Row) (interface{}, error) {
	var r ListEntry
	err := row.Scan(&r.Index, &r.Value)
	return &r, err
}

func scanMapEntry(row Row) (interface{}, error) {
	entry := new(MapEntry)
	return entry, row.Scan(&entry.Key, &entry.Value)
}

func scanDoubleMapEntry(row Row) (interface{}, error) {
	entry := new(DoubleMapEntry)
	return entry, row.Scan(&entry.FirstKey, &entry.SecondKey, &entry.Value)
}

func scanPairListEntry(row Row) (interface{}, error) {
	entry := new(PairListEntry)
	return entry, row.Scan(&entry.Index, &entry.Key, &entry.Value)
}
