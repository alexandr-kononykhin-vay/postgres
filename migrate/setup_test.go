//go:build !ci
// +build !ci

package migrate

import (
	db "github.com/alexandr-kononykhin-vay/postgres"
	"github.com/alexandr-kononykhin-vay/postgres/migrate/test"
	"github.com/joho/godotenv"
	"log"
	"os"
	"testing"
)

var testDb db.Client

func TestMain(m *testing.M) {
	testDb = setupDB()
	os.Exit(m.Run())
}

func setupDB() db.Client {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	dbc, err := test.CreateDB("dao_test", os.Getenv("DSN"))
	if err != nil {
		log.Fatalf("Failed to create database, error: %v", err)
	}

	return dbc
}
