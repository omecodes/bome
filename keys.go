package bome

type Keys struct {
	Table  string
	Fields []string
}

type ForeignKey struct {
	Name            string
	Source          *Keys
	Target          *Keys
	OnDeleteCascade bool
}
