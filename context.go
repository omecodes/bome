package bome

import "context"

type ctxTx struct{}

func ContextWithTransaction(parent context.Context, tx *TX) context.Context {
	return context.WithValue(parent, ctxTx{}, tx)
}

func Tx(ctx context.Context) *TX {
	o := ctx.Value(ctxTx{})
	if o == nil {
		return nil
	}
	return o.(*TX)
}
