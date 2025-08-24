package repository

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
	"go-gen-apis/repository/db"
	"strings"
)

type ItemRepository struct {
	db *db.DB
}

func NewItemRepository(db *db.DB) *ItemRepository {
	return &ItemRepository{db: db}
}

func (r *ItemRepository) Create(ctx context.Context, tableName string, dataArray []map[string]any) ([]map[string]any, error) {
	if len(dataArray) == 0 {
		return nil, fmt.Errorf("no data provided for creation")
	}

	columns, err := r.db.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	pkColumn, err := r.db.GetPrimaryKeyColumn(ctx, tableName)
	if err != nil {
		logrus.Warnf("could not get primary key for table %s: %v", tableName, err)
		pkColumn = "id"
	}

	var results []map[string]any

	for _, data := range dataArray {
		var insertColumns []string
		var placeholders []string
		var values []interface{}
		paramIndex := 1

		for _, col := range columns {
			if col == pkColumn {
				continue
			}
			if value, exists := data[col]; exists {
				insertColumns = append(insertColumns, col)
				placeholders = append(placeholders, fmt.Sprintf("$%d", paramIndex))
				values = append(values, value)
				paramIndex++
			}
		}

		if len(insertColumns) == 0 {
			logrus.Warnf("no valid columns found for insert in one of the data items")
			continue
		}
		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) RETURNING *",
			tableName,
			strings.Join(insertColumns, ", "),
			strings.Join(placeholders, ", "),
		)

		row := r.db.Pool.QueryRow(ctx, query, values...)

		result, err := r.parseRowToMap(row, columns)
		if err != nil {
			logrus.Errorf("failed to create item in table %s: %v", tableName, err)
			return nil, fmt.Errorf("failed to create item: %w", err)
		}
		results = append(results, result)
	}

	return results, nil
}

func (r *ItemRepository) parseRowToMap(row pgx.Row, columns []string) (map[string]any, error) {
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	err := row.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i, col := range columns {
		result[col] = r.convertValue(values[i])
	}

	return result, nil
}

func (r *ItemRepository) parseRowsToMap(rows pgx.Rows, columns []string) (map[string]interface{}, error) {
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	err := rows.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i, col := range columns {
		result[col] = r.convertValue(values[i])
	}

	return result, nil
}

func (r *ItemRepository) convertValue(value any) any {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case []byte:
		return string(v)
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case bool:
		return v
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
