package data

type BaseFrame struct {
	Name string
}

type Frame interface {
	Next() ([]interface{}, bool, error)
	Columns() []string
	Name() string
}
