package utils

import "strconv"

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
