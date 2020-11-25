package bome

type JSONMapTransaction interface {
	JSONMap
	transaction
}

type txJsonMap struct {
	*jsonMap
	tx *TX
}

func (tx *txJsonMap) Client() Client {
	return tx.tx
}

func (tx *txJsonMap) Commit() error {
	return tx.tx.Commit()
}

func (tx *txJsonMap) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *txJsonMap) TX() *TX {
	return tx.tx
}
