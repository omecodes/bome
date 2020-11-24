package bome

type JsonValueHolderTransaction interface {
	JsonValueHolder
	transaction
}

type txJsonValueHolder struct {
	JsonValueHolder
	tx *TX
}

func (s *txJsonValueHolder) Client() Client {
	return s.tx
}

func (s *txJsonValueHolder) Commit() error {
	return s.tx.Commit()
}

func (s *txJsonValueHolder) Rollback() error {
	return s.tx.Rollback()
}
