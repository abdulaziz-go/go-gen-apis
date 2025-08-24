package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"go-gen-apis/config"
	"time"
)

type DB struct {
	Pool *pgxpool.Pool
}

func NewConnection(cfg *config.GenApiConfig) (*DB, error) {
	if err := cfg.Validate(); err != nil {
		logrus.Errorf("invalid config: %v", err)
		return nil, err
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.GetConnectionString())
	if err != nil {
		logrus.Errorf("failed to parse database config: %v", err)
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	poolConfig.MaxConns = 25
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Minute * 30

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logrus.Errorf("failed to create connection pool: %v", err)
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		logrus.Errorf("failed to ping database: %v", err)
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logrus.Info("successfully connected to PostgreSQL database with pgxpool")

	return &DB{Pool: pool}, nil
}

func (db *DB) Close() {
	if db.Pool != nil {
		logrus.Info("closing database connection pool")
		db.Pool.Close()
	}
}

const GetTableInfoQuery = `
SELECT column_name 
		FROM information_schema.columns 
		WHERE table_name = $1 
		AND table_schema = 'public'
		ORDER BY ordinal_position
`

func (db *DB) GetTableInfo(ctx context.Context, tableName string) ([]string, error) {

	rows, err := db.Pool.Query(ctx, GetTableInfoQuery, tableName)
	if err != nil {
		logrus.Errorf("failed to get table info: %v", err)
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			logrus.Errorf("failed to scan column name: %v", err)
			continue
		}
		columns = append(columns, columnName)
	}

	if err = rows.Err(); err != nil {
		logrus.Errorf("rows iteration error: %v", err)
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table '%s' not found or has no columns", tableName)
	}

	return columns, nil
}

const TableExistsQuery = `
SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		)
`

func (db *DB) TableExists(ctx context.Context, tableName string) (bool, error) {
	var exists bool
	err := db.Pool.QueryRow(ctx, TableExistsQuery, tableName).Scan(&exists)
	if err != nil {
		logrus.Errorf("failed to check table existence: %v", err)
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return exists, nil
}

const GetPrimaryKeyColumnQuery = `
SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
		WHERE tc.table_name = $1 
			AND tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = 'public'
		LIMIT 1
`

func (db *DB) GetPrimaryKeyColumn(ctx context.Context, tableName string) (string, error) {
	var pkColumn string
	err := db.Pool.QueryRow(ctx, GetPrimaryKeyColumnQuery, tableName).Scan(&pkColumn)
	if err != nil {
		logrus.Errorf("failed to get primary key column: %v", err)
		return "", fmt.Errorf("failed to get primary key column: %w", err)
	}

	return pkColumn, nil
}
