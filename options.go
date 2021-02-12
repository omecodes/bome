package bome

type options struct {
	foreignKeys []ForeignKey
}

type Option func(*options)

func WithForeignKey(fk ForeignKey) Option {
	return func(o *options) {
		o.foreignKeys = append(o.foreignKeys, fk)
	}
}
