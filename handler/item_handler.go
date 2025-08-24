package handler

import (
	"fmt"
	"go-gen-apis/domains"
	"go-gen-apis/service"
	"go-gen-apis/utils"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type ItemHandler struct {
	service service.ItemService
}

func NewItemHandler(service *service.ItemService) ItemHandler {
	return ItemHandler{service: service}
}

func (h *ItemHandler) CreateItem(c *gin.Context) {
	tableName := c.Param("table_name")
	if tableName == "" {
		utils.BadRequestResponse(c, "Table name is required", nil)
		return
	}

	var req domains.CreateItemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logrus.Errorf("handler: failed to bind JSON for create request: %v", err)
		utils.BadRequestResponse(c, "Invalid request body", err)
		return
	}

	items, err := h.service.CreateItem(c.Request.Context(), tableName, &req)
	if err != nil {
		logrus.Errorf("handler: failed to create items: %v", err)
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "cannot be") {
			utils.ValidationErrorResponse(c, "Validation failed", err)
			return
		}
		utils.InternalErrorResponse(c, "Failed to create items", err)
		return
	}

	if len(items) == 1 {
		utils.CreatedResponse(c, items[0], "Item created successfully")
	} else {
		utils.ListResponse(c, items, len(items), len(items), 0, fmt.Sprintf("%d items created successfully", len(items)))
	}
}

func (h *ItemHandler) GetItemByID(c *gin.Context) {
	tableName := c.Param("table_name")
	if tableName == "" {
		utils.BadRequestResponse(c, "Table name is required", nil)
		return
	}

	id := c.Param("id")
	if id == "" {
		utils.BadRequestResponse(c, "ID is required", nil)
		return
	}

	item, err := h.service.GetSingleItem(c.Request.Context(), tableName, id)
	if err != nil {
		logrus.Errorf("handler: failed to get item by ID: %v", err)
		if strings.Contains(err.Error(), "not found") {
			utils.NotFoundResponse(c, "Item not found", err)
			return
		}
		if strings.Contains(err.Error(), "invalid") {
			utils.BadRequestResponse(c, "Invalid ID", err)
			return
		}
		utils.InternalErrorResponse(c, "Failed to get item", err)
		return
	}

	utils.SuccessResponse(c, item, "Item retrieved successfully")
}

func (h *ItemHandler) GetItems(c *gin.Context) {
	tableName := c.Param("table_name")
	if tableName == "" {
		utils.BadRequestResponse(c, "Table name is required", nil)
		return
	}

	filter := &domains.ItemFilter{
		Filters: make(map[string]interface{}),
	}

	if limitStr := c.Query("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			filter.Limit = limit
		}
	}

	if offsetStr := c.Query("offset"); offsetStr != "" {
		if offset, err := strconv.Atoi(offsetStr); err == nil {
			filter.Offset = offset
		}
	}

	filter.OrderBy = c.Query("order_by")
	filter.Sort = c.Query("sort")

	for key, values := range c.Request.URL.Query() {
		if key == "limit" || key == "offset" || key == "order_by" || key == "sort" {
			continue
		}
		if len(values) > 0 && values[0] != "" {
			value := values[0]
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				filter.Filters[key] = intVal
			} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
				filter.Filters[key] = floatVal
			} else if boolVal, err := strconv.ParseBool(value); err == nil {
				filter.Filters[key] = boolVal
			} else {
				filter.Filters[key] = value
			}
		}
	}

	items, total, err := h.service.GetItems(c.Request.Context(), tableName, filter)
	if err != nil {
		logrus.Errorf("handler: failed to get items: %v", err)
		if strings.Contains(err.Error(), "invalid") {
			utils.ValidationErrorResponse(c, "Validation failed", err)
			return
		}
		utils.InternalErrorResponse(c, "Failed to get items", err)
		return
	}

	utils.ListResponse(c, items, total, filter.Limit, filter.Offset, "Items retrieved successfully")
}

func (h *ItemHandler) UpdateItem(c *gin.Context) {
	tableName := c.Param("table_name")
	if tableName == "" {
		utils.BadRequestResponse(c, "Table name is required", nil)
		return
	}

	id := c.Param("id")
	if id == "" {
		utils.BadRequestResponse(c, "ID is required", nil)
		return
	}

	var req domains.UpdateItemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logrus.Errorf("handler: failed to bind JSON for update request: %v", err)
		utils.BadRequestResponse(c, "Invalid request body", err)
		return
	}

	item, err := h.service.UpdateItem(c.Request.Context(), tableName, id, &req)
	if err != nil {
		logrus.Errorf("handler: failed to update item: %v", err)
		if strings.Contains(err.Error(), "not found") {
			utils.NotFoundResponse(c, "Item not found", err)
			return
		}
		if strings.Contains(err.Error(), "invalid") {
			utils.ValidationErrorResponse(c, "Validation failed", err)
			return
		}
		utils.InternalErrorResponse(c, "Failed to update item", err)
		return
	}

	utils.SuccessResponse(c, item, "Item updated successfully")
}

func (h *ItemHandler) DeleteItem(c *gin.Context) {
	tableName := c.Param("table_name")
	if tableName == "" {
		utils.BadRequestResponse(c, "Table name is required", nil)
		return
	}

	id := c.Param("id")
	if id == "" {
		utils.BadRequestResponse(c, "ID is required", nil)
		return
	}

	err := h.service.DeleteItem(c.Request.Context(), tableName, id)
	if err != nil {
		logrus.Errorf("handler: failed to delete item: %v", err)
		if strings.Contains(err.Error(), "not found") {
			utils.NotFoundResponse(c, "Item not found", err)
			return
		}
		if strings.Contains(err.Error(), "invalid") {
			utils.BadRequestResponse(c, "Invalid ID", err)
			return
		}
		utils.InternalErrorResponse(c, "Failed to delete item", err)
		return
	}

	utils.DeletedResponse(c, "Item deleted successfully")
}
