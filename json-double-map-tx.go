package bome

// JSONDoubleMapTransaction is a convenience for double mapping persistent store
type JSONDoubleMapTransaction interface {
	JSONDoubleMap
	transaction
}

type txJsonDoubleMap struct {
	JSONDoubleMap
	tx *TX
}

func (s *txJsonDoubleMap) Client() Client {
	return s.tx
}

func (s *txJsonDoubleMap) Commit() error {
	return s.tx.Commit()
}

func (s *txJsonDoubleMap) Rollback() error {
	return s.tx.Rollback()
}
