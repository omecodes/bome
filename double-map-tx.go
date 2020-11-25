package bome

type DoubleMapTransaction interface {
	DoubleMap
	transaction
}

type txDoubleMap struct {
	*doubleMap
	tx *TX
}

func (tx *txDoubleMap) Client() Client {
	return tx.tx
}

func (tx *txDoubleMap) Commit() error {
	return tx.tx.Commit()
}

func (tx *txDoubleMap) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *txDoubleMap) TX() *TX {
	return tx.tx
}
