package engine

type DbType int

const (
	IntType DbType = iota
	StringType
)

type ColumnDef struct {
	Name         string
	Type         DbType
	IsPrimaryKey bool
	IsUnique     bool
}

type TableSchema struct {
	Name    string
	Columns []ColumnDef
}

type Row struct {
	Id        int                    `json:"id"`
	Data      map[string]interface{} `json:"data"`
	IsDeleted bool                   `json:"-"`
}

func NewRow() *Row {
	return &Row{
		Data: make(map[string]interface{}),
	}
}