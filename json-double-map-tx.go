package bome

// JSONDoubleMapTransaction
type JSONDoubleMapTransaction interface {
	JSONDoubleMap
	transaction
}

type txJsonDoubleMap struct {
	*jsonDoubleMap
	tx *TX
}

func (tx *txJsonDoubleMap) Client() Client {
	return tx.tx
}

func (tx *txJsonDoubleMap) Commit() error {
	return tx.tx.Commit()
}

func (tx *txJsonDoubleMap) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *txJsonDoubleMap) TX() *TX {
	return tx.tx
}
