package bome

type transaction interface {
	Commit() error
	Rollback() error
}
