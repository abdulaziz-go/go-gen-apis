package repository

import (
	"context"
	"fmt"
	"github.com/abdulaziz-go/github.com/abdulaziz-go/go-gen-apis/domains"
	"github.com/abdulaziz-go/go-gen-apis/repository/db"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
	"strconv"
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

func (r *ItemRepository) GetByID(ctx context.Context, tableName string, id interface{}) (map[string]interface{}, error) {
	columns, err := r.db.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	pkColumn, err := r.db.GetPrimaryKeyColumn(ctx, tableName)
	if err != nil {
		logrus.Warnf("could not get primary key for table %s: %v", tableName, err)
		pkColumn = "id"
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = $1", tableName, pkColumn)

	row := r.db.Pool.QueryRow(ctx, query, id)

	result, err := r.parseRowToMap(row, columns)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("item not found")
		}
		logrus.Errorf("failed to get item by ID from table %s: %v", tableName, err)
		return nil, fmt.Errorf("failed to get item: %w", err)
	}

	return result, nil
}

func (r *ItemRepository) GetAll(ctx context.Context, tableName string, filter *domains.ItemFilter) ([]map[string]interface{}, int, error) {
	columns, err := r.db.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get table info: %w", err)
	}

	baseQuery := fmt.Sprintf("FROM %s", tableName)
	var whereConditions []string
	var args []interface{}
	argIndex := 1

	if filter.Filters != nil && len(filter.Filters) > 0 {
		for column, value := range filter.Filters {
			if r.columnExists(columns, column) {
				whereConditions = append(whereConditions, fmt.Sprintf("%s = $%d", column, argIndex))
				args = append(args, value)
				argIndex++
			}
		}
	}

	if len(whereConditions) > 0 {
		baseQuery += " WHERE " + strings.Join(whereConditions, " AND ")
	}

	countQuery := "SELECT COUNT(*) " + baseQuery
	var total int
	err = r.db.Pool.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		logrus.Errorf("failed to count items in table %s: %v", tableName, err)
		return nil, 0, fmt.Errorf("failed to count items: %w", err)
	}

	selectQuery := "SELECT * " + baseQuery

	if filter.OrderBy != "" && r.columnExists(columns, filter.OrderBy) {
		sort := domains.SORT_ASC
		if strings.ToUpper(filter.Sort) == domains.SORT_DESC {
			sort = domains.SORT_DESC
		}
		selectQuery += fmt.Sprintf(" ORDER BY %s %s", filter.OrderBy, sort)
	} else {
		selectQuery += fmt.Sprintf(" ORDER BY %s ASC", columns[0])
	}

	if filter.Limit > 0 {
		selectQuery += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, filter.Limit)
		argIndex++
	}

	if filter.Offset > 0 {
		selectQuery += fmt.Sprintf(" OFFSET $%d", argIndex)
		args = append(args, filter.Offset)
	}

	rows, err := r.db.Pool.Query(ctx, selectQuery, args...)
	if err != nil {
		logrus.Errorf("failed to query items from table %s: %v", tableName, err)
		return nil, 0, fmt.Errorf("failed to query items: %w", err)
	}
	defer rows.Close()

	var items []map[string]interface{}
	for rows.Next() {
		item, err := r.parseRowsToMap(rows, columns)
		if err != nil {
			logrus.Errorf("failed to scan item from table %s: %v", tableName, err)
			continue
		}
		items = append(items, item)
	}

	if err = rows.Err(); err != nil {
		logrus.Errorf("rows iteration error for table %s: %v", tableName, err)
		return nil, 0, fmt.Errorf("rows iteration error: %w", err)
	}

	return items, total, nil
}

func (r *ItemRepository) Update(ctx context.Context, tableName string, id interface{}, data map[string]interface{}) (map[string]interface{}, error) {
	columns, err := r.db.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	pkColumn, err := r.db.GetPrimaryKeyColumn(ctx, tableName)
	if err != nil {
		logrus.Warnf("could not get primary key for table %s: %v", tableName, err)
		pkColumn = "id"
	}

	var updateColumns []string
	var values []interface{}
	paramIndex := 1

	for _, col := range columns {
		if col == pkColumn {
			continue
		}
		if value, exists := data[col]; exists {
			updateColumns = append(updateColumns, fmt.Sprintf("%s = $%d", col, paramIndex))
			values = append(values, value)
			paramIndex++
		}
	}

	if len(updateColumns) == 0 {
		return nil, fmt.Errorf("no valid columns found for update")
	}

	values = append(values, id)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = $%d RETURNING *",
		tableName,
		strings.Join(updateColumns, ", "),
		pkColumn,
		paramIndex,
	)

	row := r.db.Pool.QueryRow(ctx, query, values...)

	result, err := r.parseRowToMap(row, columns)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("item not found")
		}
		logrus.Errorf("failed to update item in table %s: %v", tableName, err)
		return nil, fmt.Errorf("failed to update item: %w", err)
	}

	return result, nil
}

func (r *ItemRepository) Delete(ctx context.Context, tableName string, id interface{}) error {
	pkColumn, err := r.db.GetPrimaryKeyColumn(ctx, tableName)
	if err != nil {
		logrus.Warnf("could not get primary key for table %s: %v", tableName, err)
		pkColumn = "id"
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", tableName, pkColumn)

	result, err := r.db.Pool.Exec(ctx, query, id)
	if err != nil {
		logrus.Errorf("failed to delete item from table %s: %v", tableName, err)
		return fmt.Errorf("failed to delete item: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("item not found")
	}

	logrus.Infof("successfully deleted item from table: %s", tableName)
	return nil
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

func (r *ItemRepository) columnExists(columns []string, column string) bool {
	for _, col := range columns {
		if col == column {
			return true
		}
	}
	return false
}

func (r *ItemRepository) convertID(id string) interface{} {
	if intID, err := strconv.ParseInt(id, 10, 64); err == nil {
		return intID
	}
	return id
}
