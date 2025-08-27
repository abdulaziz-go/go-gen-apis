package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/abdulaziz-go/go-gen-apis/domains"
	"github.com/abdulaziz-go/go-gen-apis/repository/db"
	"github.com/abdulaziz-go/go-gen-apis/utils"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
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
		var values []any
		paramIndex := 1

		for _, col := range columns {
			if col == pkColumn {
				continue
			}
			if value, exists := data[col]; exists {
				insertColumns = append(insertColumns, r.quoteIdentifier(col))
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
			r.quoteIdentifier(tableName),
			strings.Join(insertColumns, ", "),
			strings.Join(placeholders, ", "),
		)

		row := r.db.Pool.QueryRow(ctx, query, values...)

		result, err := r.parseRowToMap(row, columns, tableName)
		if err != nil {
			logrus.Errorf("failed to create item in table %s: %v", tableName, err)
			return nil, fmt.Errorf("failed to create item: %w", err)
		}
		results = append(results, result)
	}

	return results, nil
}

func (r *ItemRepository) GetByID(ctx context.Context, tableName string, id any) (map[string]any, error) {
	columns, err := r.db.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	pkColumn, err := r.db.GetPrimaryKeyColumn(ctx, tableName)
	if err != nil {
		logrus.Warnf("could not get primary key for table %s: %v", tableName, err)
		pkColumn = "id"
	}

	var selectColumns []string
	for _, col := range columns {
		selectColumns = append(selectColumns, fmt.Sprintf("%s::text as %s", r.quoteIdentifier(col), r.quoteIdentifier(col)))
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		strings.Join(selectColumns, ", "), r.quoteIdentifier(tableName), r.quoteIdentifier(pkColumn))

	row := r.db.Pool.QueryRow(ctx, query, id)

	result, err := r.parseRowToMap(row, columns, tableName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("item not found")
		}
		logrus.Errorf("failed to get item by ID from table %s: %v", tableName, err)
		return nil, fmt.Errorf("failed to get item: %w", err)
	}

	return result, nil
}

func (r *ItemRepository) GetAll(ctx context.Context, tableName string, filter *domains.ItemFilter) ([]map[string]any, int, error) {
	columns, err := r.db.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get table info: %w", err)
	}

	baseQuery := fmt.Sprintf("FROM %s", r.quoteIdentifier(tableName))
	var whereConditions []string
	var args []any
	argIndex := 1

	if filter.Filters != nil && len(filter.Filters) > 0 {
		for column, value := range filter.Filters {
			if r.columnExists(columns, column) {
				switch v := value.(type) {
				case []any:
					placeholders := make([]string, len(v))
					for i := range v {
						placeholders[i] = fmt.Sprintf("$%d", argIndex+i)
					}
					whereConditions = append(whereConditions,
						fmt.Sprintf("%s IN (%s)", r.quoteIdentifier(column), strings.Join(placeholders, ",")))
					args = append(args, v...)
					argIndex += len(v)
				default:
					whereConditions = append(whereConditions, fmt.Sprintf("%s = $%d", r.quoteIdentifier(column), argIndex))
					args = append(args, v)
					argIndex++
				}
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

	var selectColumns []string
	for _, col := range columns {
		selectColumns = append(selectColumns, fmt.Sprintf("%s::text as %s", r.quoteIdentifier(col), r.quoteIdentifier(col)))
	}

	selectQuery := fmt.Sprintf("SELECT %s %s", strings.Join(selectColumns, ", "), baseQuery)

	if filter.OrderBy != "" && r.columnExists(columns, filter.OrderBy) {
		sort := domains.SORT_ASC
		if strings.ToUpper(filter.Sort) == domains.SORT_DESC {
			sort = domains.SORT_DESC
		}
		selectQuery += fmt.Sprintf(" ORDER BY %s %s", r.quoteIdentifier(filter.OrderBy), sort)
	} else {
		selectQuery += fmt.Sprintf(" ORDER BY %s ASC", r.quoteIdentifier(columns[0]))
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

	var items []map[string]any
	for rows.Next() {
		item, err := r.parseRowsToMap(rows, columns, tableName)
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

func (r *ItemRepository) Update(ctx context.Context, tableName string, id any, data map[string]any) (map[string]any, error) {
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
	var values []any
	paramIndex := 1

	for _, col := range columns {
		if col == pkColumn {
			continue
		}
		if value, exists := data[col]; exists {
			updateColumns = append(updateColumns, fmt.Sprintf("%s = $%d", r.quoteIdentifier(col), paramIndex))
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
		r.quoteIdentifier(tableName),
		strings.Join(updateColumns, ", "),
		r.quoteIdentifier(pkColumn),
		paramIndex,
	)

	row := r.db.Pool.QueryRow(ctx, query, values...)

	result, err := r.parseRowToMap(row, columns, tableName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("item not found")
		}
		logrus.Errorf("failed to update item in table %s: %v", tableName, err)
		return nil, fmt.Errorf("failed to update item: %w", err)
	}

	return result, nil
}

func (r *ItemRepository) Delete(ctx context.Context, tableName string, id any) error {
	pkColumn, err := r.db.GetPrimaryKeyColumn(ctx, tableName)
	if err != nil {
		logrus.Warnf("could not get primary key for table %s: %v", tableName, err)
		pkColumn = "id"
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", r.quoteIdentifier(tableName), r.quoteIdentifier(pkColumn))

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

const GetColumnTypeQuery = `
SELECT 
    column_name, 
    data_type,
    udt_name,
    CASE 
        WHEN data_type = 'USER-DEFINED' THEN udt_name
        ELSE data_type 
    END as actual_type
FROM information_schema.columns 
WHERE table_name = $1 AND table_schema = 'public'
`

func (r *ItemRepository) getColumnTypes(ctx context.Context, tableName string) (map[string]string, error) {
	rows, err := r.db.Pool.Query(ctx, GetColumnTypeQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes := make(map[string]string)
	for rows.Next() {
		var columnName, dataType, udtName, actualType string
		if err := rows.Scan(&columnName, &dataType, &udtName, &actualType); err != nil {
			return nil, err
		}
		columnTypes[columnName] = actualType
	}

	return columnTypes, nil
}

func (r *ItemRepository) shouldParseAsJSON(columnName, dataType string, value any) bool {
	if dataType != "jsonb" {
		return false
	}

	if strings.Contains(strings.ToLower(columnName), "id") {
		return false
	}

	if value == nil {
		return false
	}

	if strValue, ok := value.(string); ok {
		strValue = strings.TrimSpace(strValue)
		return strValue != "" && (strings.HasPrefix(strValue, "{") || strings.HasPrefix(strValue, "["))
	}

	return false
}

func (r *ItemRepository) parseRowToMap(row pgx.Row, columns []string, tableName string) (map[string]any, error) {
	columnTypes, err := r.getColumnTypes(context.Background(), tableName)
	if err != nil {
		logrus.Warnf("could not get column types for table %s: %v", tableName, err)
		columnTypes = make(map[string]string)
	}

	values := make([]any, len(columns))
	scanTargets := make([]any, len(columns))

	for i := range values {
		scanTargets[i] = &values[i]
	}

	if err := row.Scan(scanTargets...); err != nil {
		return nil, err
	}

	result := make(map[string]any)
	for i, column := range columns {
		value := values[i]

		processedValue := r.processColumnValue(value, column, columnTypes[column])

		if r.shouldParseAsJSON(column, columnTypes[column], processedValue) {
			if strValue, ok := processedValue.(string); ok && strValue != "" {
				var jsonValue any
				if err := json.Unmarshal([]byte(strValue), &jsonValue); err != nil {
					logrus.Warnf("failed to unmarshal JSONB field %s: %v", column, err)
					result[column] = processedValue
				} else {
					result[column] = jsonValue
				}
			} else {
				result[column] = processedValue
			}
		} else {
			result[column] = processedValue
		}
	}

	return result, nil
}

func (r *ItemRepository) parseRowsToMap(rows pgx.Rows, columns []string, tableName string) (map[string]any, error) {
	columnTypes, err := r.getColumnTypes(context.Background(), tableName)
	if err != nil {
		logrus.Warnf("could not get column types for table %s: %v", tableName, err)
		columnTypes = make(map[string]string)
	}

	values := make([]any, len(columns))
	scanTargets := make([]any, len(columns))

	for i := range values {
		scanTargets[i] = &values[i]
	}

	if err := rows.Scan(scanTargets...); err != nil {
		return nil, err
	}

	result := make(map[string]any)
	for i, column := range columns {
		value := values[i]

		processedValue := r.processColumnValue(value, column, columnTypes[column])

		if r.shouldParseAsJSON(column, columnTypes[column], processedValue) {
			if strValue, ok := processedValue.(string); ok && strValue != "" {
				var jsonValue any
				if err := json.Unmarshal([]byte(strValue), &jsonValue); err != nil {
					logrus.Warnf("failed to unmarshal JSONB field %s: %v", column, err)
					result[column] = processedValue
				} else {
					result[column] = jsonValue
				}
			} else {
				result[column] = processedValue
			}
		} else {
			result[column] = processedValue
		}
	}

	return result, nil
}

func (r *ItemRepository) processColumnValue(value any, columnName, dataType string) any {
	if value == nil {
		return nil
	}

	if strings.HasSuffix(dataType, "[]") {
		return r.parsePostgreSQLArray(value)
	}

	switch v := value.(type) {
	case string:
		return utils.ConvertStringToAppropriateType(v, dataType)
	case []byte:
		strValue := string(v)
		return utils.ConvertStringToAppropriateType(strValue, dataType)
	default:
		return utils.ConvertValue(v)
	}
}

func (r *ItemRepository) parsePostgreSQLArray(value any) []string {
	if value == nil {
		return []string{}
	}

	var strValue string
	switch v := value.(type) {
	case string:
		strValue = v
	case []byte:
		strValue = string(v)
	default:
		return []string{}
	}

	if strValue == "{}" || strValue == "" {
		return []string{}
	}

	if strings.HasPrefix(strValue, "{") && strings.HasSuffix(strValue, "}") {
		strValue = strValue[1 : len(strValue)-1]
	}

	var elements []string
	var current strings.Builder
	inQuotes := false

	for _, char := range strValue {
		switch char {
		case '"':
			inQuotes = !inQuotes
		case ',':
			if !inQuotes {
				elements = append(elements, current.String())
				current.Reset()
			} else {
				current.WriteRune(char)
			}
		default:
			current.WriteRune(char)
		}
	}

	if current.Len() > 0 {
		elements = append(elements, current.String())
	}

	return elements
}

func (r *ItemRepository) columnExists(columns []string, column string) bool {
	for _, col := range columns {
		if col == column {
			return true
		}
	}
	return false
}

func (r *ItemRepository) quoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}
