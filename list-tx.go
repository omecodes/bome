package bome

type ListTransaction interface {
	List
	transaction
}

type txList struct {
	*listDB
	tx *TX
}

func (tx *txList) Client() Client {
	return tx.tx
}

func (tx *txList) Commit() error {
	return tx.tx.Commit()
}

func (tx *txList) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *txList) TX() *TX {
	return tx.tx
}
