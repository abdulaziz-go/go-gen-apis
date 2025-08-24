package service

import (
	"context"
	"fmt"
	"go-gen-apis/domains"
	"go-gen-apis/repository"
	"regexp"
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

	return nil, nil
}

func (s *ItemService) GetSingleItem(ctx context.Context, tableName string, id string) (map[string]any, error) {
	return nil, nil
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
