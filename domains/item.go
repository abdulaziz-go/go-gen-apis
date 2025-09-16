package domains

import "time"

const (
	SORT_ASC  = "ASC"
	SORT_DESC = "DESC"
)

type GenericItem struct {
	Data      map[string]any `json:"data"`
	TableName string         `json:"table_name,omitempty"`
}

type CreateItemRequest struct {
	Data []map[string]any `json:"data" binding:"required"`
}

type UpdateItemRequest struct {
	Data map[string]any `json:"data" binding:"required"`
}

type ItemFilter struct {
	Limit   int            `json:"limit" form:"limit"`
	Offset  int            `json:"offset" form:"offset"`
	OrderBy string         `json:"order_by" form:"order_by"`
	Sort    string         `json:"sort" form:"sort"`
	Filters map[string]any `json:"filters" form:"filters"`
	Search  string         `json:"search" form:"search"`
}

type ItemResponse struct {
	Success bool           `json:"success"`
	Data    map[string]any `json:"data,omitempty"`
	Message string         `json:"message,omitempty"`
	Error   string         `json:"error,omitempty"`
}

type ItemsListResponse struct {
	Success bool             `json:"success"`
	Data    []map[string]any `json:"data"`
	Total   int              `json:"total"`
	Limit   int              `json:"limit"`
	Offset  int              `json:"offset"`
	Message string           `json:"message,omitempty"`
	Error   string           `json:"error,omitempty"`
}

type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

type DatabaseColumn struct {
	Name         string `db:"column_name"`
	DataType     string `db:"data_type"`
	IsNullable   string `db:"is_nullable"`
	DefaultValue string `db:"column_default"`
}

type TableInfo struct {
	Name       string           `json:"name"`
	Columns    []DatabaseColumn `json:"columns"`
	PrimaryKey string           `json:"primary_key"`
}

type TimeFields struct {
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}
