package bome

type MapTransaction interface {
	Map
	transaction
}

type txDict struct {
	*dict
	tx *TX
}

func (tx *txDict) Client() Client {
	return tx.tx
}

func (tx *txDict) Commit() error {
	return tx.tx.Commit()
}

func (tx *txDict) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *txDict) TX() *TX {
	return tx.tx
}
