package bome

import (
	"context"
	"database/sql"
)

type ctxTx struct{}
type ctxWithTransaction struct{}

func contextWithTransaction(parent context.Context, tx *TX) context.Context {
	newCtx := context.WithValue(parent, ctxTx{}, tx)
	newCtx = context.WithValue(newCtx, ctxWithTransaction{}, true)
	return newCtx
}

func transaction(ctx context.Context) *TX {
	o := ctx.Value(ctxTx{})
	if o == nil {
		return nil
	}
	return o.(*TX)
}

func TransactionsCommit(ctx context.Context) error {
	tx := transaction(ctx)
	if tx == nil {
		return TransactionNotFound
	}
	return tx.Commit()
}

func TransactionExec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tx := transaction(ctx)
	if tx == nil {
		return nil, TransactionNotFound
	}
	return tx.Exec(query, args...)
}

func TransactionQuery(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	tx := transaction(ctx)
	if tx == nil {
		return nil, TransactionNotFound
	}
	return tx.Query(query, args...)
}

func TransactionRollback(ctx context.Context) error {
	tx := transaction(ctx)
	if tx == nil {
		return TransactionNotFound
	}
	return tx.Rollback()
}

func IsTransactionContext(ctx context.Context) bool {
	return ctx.Value(ctxTx{}) != nil
}
