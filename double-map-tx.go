package bome

type DoubleMapTransaction interface {
	DoubleMap
	transaction
}

type txDoubleMap struct {
	*doubleMap
	tx *TX
}

func (s *txDoubleMap) Client() Client {
	return s.tx
}

func (s *txDoubleMap) Commit() error {
	return s.tx.Commit()
}

func (s *txDoubleMap) Rollback() error {
	return s.tx.Rollback()
}
