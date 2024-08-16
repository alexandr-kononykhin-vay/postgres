package test

import (
	"github.com/go-pg/pg/v10"
	"strings"
	"testing"

	db "github.com/alexandr-kononykhin-vay/postgres"
)

// CreateDB creates database if not exists by DSN
func CreateDB(appName string, dsn string) (db.Client, error) {
	opts, err := pg.ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	dbc := db.Connect(appName, opts)
	_, err = dbc.Db().ExecOne("SELECT 1")
	if err == nil {
		return dbc, nil
	}

	targetDB := opts.Database
	opts.Database = "postgres"
	dbConn := pg.Connect(opts)

	_, err = dbConn.Exec("CREATE DATABASE " + targetDB)
	if err != nil {
		return nil, err
	}

	err = dbConn.Close()
	if err != nil {
		return nil, err
	}

	opts.Database = targetDB
	return db.Connect(appName, opts), nil
}

// CleanDB truncates all tables except of 'gopg_migrations'
func CleanDB(dbc db.Client, t *testing.T) {
	tables := pg.Strings{}

	_, err := dbc.Query(&tables, `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name!='gopg_migrations'`)
	if err != nil {
		t.Fatalf("Failed to get tables list, error: %v", err)
	}

	if len(tables) > 0 {
		_, err = dbc.Exec("TRUNCATE " + strings.Join(tables, ",") + " CASCADE")
		if err != nil {
			t.Fatalf("Failed to clean tables, error: %v", err)
		}
	}
}
