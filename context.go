package bome

import (
	"context"
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
