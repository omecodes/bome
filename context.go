package bome

import (
	"context"
)

type transactionActions struct {
	Commits   []ActionFunc
	Rollbacks []ActionFunc
}

type ctxTx struct{}

type ctxTransactionActions struct{}

type ActionFunc func() error

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

func ContextWithCommitActions(parent context.Context, actions ...ActionFunc) context.Context {
	var ta *transactionActions
	o := parent.Value(ctxTransactionActions{})
	if o == nil {
		ta = new(transactionActions)
	} else {
		ta = o.(*transactionActions)
	}

	for _, action := range actions {
		ta.Commits = append(ta.Commits, action)
	}

	if o == nil {
		return context.WithValue(parent, ctxTransactionActions{}, ta)
	}
	return parent
}

func ContextWithRollbackActions(parent context.Context, actions ...ActionFunc) context.Context {
	var ta *transactionActions
	o := parent.Value(ctxTransactionActions{})
	if o == nil {
		ta = new(transactionActions)
	} else {
		ta = o.(*transactionActions)
	}

	for _, action := range actions {
		ta.Rollbacks = append(ta.Rollbacks, action)
	}

	if o == nil {
		return context.WithValue(parent, ctxTransactionActions{}, ta)
	}
	return parent
}

func Transaction(parent context.Context) context.Context {
	ta := new(transactionActions)
	return context.WithValue(parent, ctxTransactionActions{}, ta)
}

func Commit(ctx context.Context) error {
	tx := transaction(ctx)
	if tx != nil {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}

	o := ctx.Value(ctxTransactionActions{})
	if o != nil {
		ta := o.(*transactionActions)
		for _, action := range ta.Commits {
			if err := action(); err != nil {
				return err
			}
		}
	}
	return nil
}

func Rollback(ctx context.Context) error {
	tx := transaction(ctx)
	if tx != nil {
		err := tx.Rollback()
		if err != nil {
			return err
		}
	}

	o := ctx.Value(ctxTransactionActions{})
	if o != nil {
		ta := o.(*transactionActions)
		for _, action := range ta.Rollbacks {
			if err := action(); err != nil {
				return err
			}
		}
	}

	return nil
}
