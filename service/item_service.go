package service

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"go-gen-apis/domains"
	"go-gen-apis/repository"
	"regexp"
	"strconv"
	"strings"
)

type ItemService struct {
	repo *repository.ItemRepository
}

func NewItemService(repo *repository.ItemRepository) *ItemService {
	return &ItemService{repo: repo}
}

func (s *ItemService) CreateItem(ctx context.Context, tableName string, req *domains.CreateItemRequest) ([]map[string]any, error) {
	if err := s.validTableName(tableName); err != nil {
		return nil, err
	}

	if err := s.validateCreateRequest(req); err != nil {
		return nil, err
	}
	items, err := s.repo.Create(ctx, tableName, req.Data)
	if err != nil {
		logrus.Errorf("service: failed to create items in table %s: %v", tableName, err)
		return nil, fmt.Errorf("failed to create items: %w", err)
	}

	return items, nil
}

func (s *ItemService) GetSingleItem(ctx context.Context, tableName string, idString string) (map[string]any, error) {
	if err := s.validTableName(tableName); err != nil {
		return nil, err
	}

	id, err := s.convertAndValidateID(idString)
	if err != nil {
		return nil, err
	}

	fmt.Println(id)
}

func (s *ItemService) GetItems(ctx context.Context, tableName string, filter *domains.ItemFilter) ([]map[string]any, int, error) {
	return nil, 0, nil
}

func (s *ItemService) UpdateItem(ctx context.Context, tableName string, id string, req *domains.UpdateItemRequest) (map[string]any, error) {
	return nil, nil
}

func (s *ItemService) DeleteItem(ctx context.Context, tableName string, id string) error {
	return nil
}

func (s *ItemService) validTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if len(tableName) > 63 {
		return fmt.Errorf("table name too long: maximum 63 character")
	}

	matched, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_]*$`, tableName)

	if !matched {
		return fmt.Errorf("invalid table name alphanumeric or underscare is required")
	}

	return nil
}

func (s *ItemService) validateCreateRequest(req *domains.CreateItemRequest) error {
	if req == nil {
		return fmt.Errorf("request cannot be nil")
	}

	if req.Data == nil || len(req.Data) == 0 {
		return fmt.Errorf("data cannot be empty")
	}

	if len(req.Data) > 100 {
		return fmt.Errorf("too many fields: maximum 100 fields allowed")
	}

	return nil
}

func (s *ItemService) validateUpdateRequest(req *domains.UpdateItemRequest) error {
	if req == nil {
		return fmt.Errorf("request cannot be nil")
	}

	if req.Data == nil || len(req.Data) == 0 {
		return fmt.Errorf("data cannot be empty")
	}

	if len(req.Data) > 100 {
		return fmt.Errorf("too many fields: maximum 100 fields allowed")
	}

	return nil
}

func validateAndNormalizeFilter(filter *domains.ItemFilter) error {
	if filter == nil {
		return fmt.Errorf("filter cannot be nil")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Limit > 1000 {
		filter.Limit = 1000 // for preventing performance problems
	}

	if filter.Offset < 0 {
		filter.Offset = 0
	}

	if filter.OrderBy != "" {
		matched, _ := regexp.MatchString("^[a-zA-Z_][a-zA-Z0-9_]*$", filter.OrderBy)
		if !matched {
			return fmt.Errorf("invalid order by column name")
		}
	}

	if filter.Sort != "" {
		sort := strings.ToUpper(filter.Sort)
		if sort != domains.SORT_ASC && sort != domains.SORT_DESC {
			return fmt.Errorf("invalid sort direction: must be ASC or DESC")
		}
		filter.Sort = sort
	}

	return nil
}

func (s *ItemService) convertAndValidateID(idStr string) (interface{}, error) {
	if idStr == "" {
		return nil, fmt.Errorf("ID cannot be empty")
	}

	if intID, err := strconv.ParseInt(idStr, 10, 64); err == nil {
		if intID <= 0 {
			return nil, fmt.Errorf("invalid ID: must be positive")
		}
		return intID, nil
	}

	if len(idStr) > 255 {
		return nil, fmt.Errorf("ID too long")
	}

	matched, _ := regexp.MatchString("^[a-zA-Z0-9_-]+$", idStr)
	if !matched {
		return nil, fmt.Errorf("invalid ID format: only alphanumeric characters, underscores, and hyphens allowed")
	}

	return idStr, nil
}
