package utils

import (
	"github.com/abdulaziz-go/go-gen-apis/domains"
	"github.com/gin-gonic/gin"
	"net/http"
)

func SuccessResponse(c *gin.Context, data any, message string) {
	response := domains.ItemResponse{
		Success: true,
		Data:    data.(map[string]any),
		Message: message,
	}
	c.JSON(http.StatusOK, response)
}

func CreatedResponse(c *gin.Context, data any, message string) {
	response := domains.ItemResponse{
		Success: true,
		Data:    data.(map[string]any),
		Message: message,
	}
	c.JSON(http.StatusCreated, response)
}

func ListResponse(c *gin.Context, data []map[string]any, total, limit, offset int, message string) {
	response := domains.ItemsListResponse{
		Success: true,
		Data:    data,
		Total:   total,
		Limit:   limit,
		Offset:  offset,
		Message: message,
	}
	c.JSON(http.StatusOK, response)
}

func ErrorResponse(c *gin.Context, statusCode int, message string, err error) {
	response := domains.ErrorResponse{
		Success: false,
		Error:   message,
	}

	if err != nil {
		response.Message = err.Error()
	}

	c.JSON(statusCode, response)
}

func BadRequestResponse(c *gin.Context, message string, err error) {
	ErrorResponse(c, http.StatusBadRequest, message, err)
}

func NotFoundResponse(c *gin.Context, message string, err error) {
	ErrorResponse(c, http.StatusNotFound, message, err)
}

func InternalErrorResponse(c *gin.Context, message string, err error) {
	ErrorResponse(c, http.StatusInternalServerError, message, err)
}

func ValidationErrorResponse(c *gin.Context, message string, err error) {
	ErrorResponse(c, http.StatusUnprocessableEntity, message, err)
}

func ConflictResponse(c *gin.Context, message string, err error) {
	ErrorResponse(c, http.StatusConflict, message, err)
}

func DeletedResponse(c *gin.Context, message string) {
	response := domains.ItemResponse{
		Success: true,
		Message: message,
	}
	c.JSON(http.StatusOK, response)
}
