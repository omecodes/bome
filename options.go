package bome

type options struct {
	foreignKeys []*ForeignKey
	indexes     []*Index
}

type Option func(*options)

func WithForeignKey(fk ...*ForeignKey) Option {
	return func(o *options) {
		o.foreignKeys = append(o.foreignKeys, fk...)
	}
}

func WithIndex(indexes ...*Index) Option {
	return func(o *options) {
		o.indexes = append(o.indexes, indexes...)
	}
}
