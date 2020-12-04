package bome

import "context"

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

func CommitTransactions(ctx context.Context) error {
	tx := transaction(ctx)
	if tx == nil {
		return TransactionNotFound
	}
	return tx.Commit()
}

func RollbackTransactions(ctx context.Context) error {
	tx := transaction(ctx)
	if tx == nil {
		return TransactionNotFound
	}
	return tx.Rollback()
}

func IsTransactionContext(ctx context.Context) bool {
	o := ctx.Value(ctxWithTransaction{})
	if o == nil {
		return false
	}
	return true
}
