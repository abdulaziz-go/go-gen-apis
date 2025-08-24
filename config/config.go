package config

import "fmt"

type GenApiConfig struct {
	PostgresUrl      string
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string
	Port             string
}

func (c *GenApiConfig) GetConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		c.PostgresUser, c.PostgresPassword, c.PostgresUrl, c.PostgresDB)
}

func (c *GenApiConfig) Validate() error {
	if c.PostgresUrl == "" {
		return fmt.Errorf("postgres URL is required")
	}
	if c.PostgresUser == "" {
		return fmt.Errorf("postgres user is required")
	}
	if c.PostgresDB == "" {
		return fmt.Errorf("postgres database name is required")
	}
	return nil
}
