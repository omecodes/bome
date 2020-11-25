package bome

type JsonListTransaction interface {
	JSONList
	transaction
}

type txJsonList struct {
	*jsonList
	tx *TX
}

func (tx *txJsonList) Client() Client {
	return tx.tx
}

func (tx *txJsonList) Commit() error {
	return tx.tx.Commit()
}

func (tx *txJsonList) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *txJsonList) TX() *TX {
	return tx.tx
}
