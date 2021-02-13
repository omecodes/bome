package bome

import (
	"context"
)

type ctxTx struct{}

func contextWithTransaction(parent context.Context, tx *TX) context.Context {
	return context.WithValue(parent, ctxTx{}, tx)
}

func transaction(ctx context.Context) *TX {
	o := ctx.Value(ctxTx{})
	if o == nil {
		return nil
	}
	return o.(*TX)
}

func Commit(ctx context.Context) error {
	tx := transaction(ctx)
	if tx != nil {
		return tx.Commit()
	}
	return nil
}

func Rollback(ctx context.Context) error {
	tx := transaction(ctx)
	if tx != nil {
		return tx.Rollback()
	}
	return nil
}
