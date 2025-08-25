package utils

import (
	"fmt"
	"strconv"
)

func ParseValue(value string) interface{} {
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		return intVal
	}
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		return floatVal
	}
	if boolVal, err := strconv.ParseBool(value); err == nil {
		return boolVal
	}
	return value
}

func ConvertValue(value any) any {
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

func ConvertStringToAppropriateType(strValue, dataType string) any {
	if strValue == "" {
		return nil
	}

	switch dataType {
	case "integer", "int4", "int8", "bigint", "smallint":
		if intVal, err := strconv.ParseInt(strValue, 10, 64); err == nil {
			return intVal
		}
	case "numeric", "decimal", "real", "double precision", "float4", "float8":
		if floatVal, err := strconv.ParseFloat(strValue, 64); err == nil {
			return floatVal
		}
	case "boolean", "bool":
		if boolVal, err := strconv.ParseBool(strValue); err == nil {
			return boolVal
		}
	case "uuid":
		return strValue
	case "jsonb", "json":
		return strValue
	default:
		return strValue
	}

	return strValue
}
