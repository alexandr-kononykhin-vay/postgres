//go:build !ci
// +build !ci

package migrate

import (
	"github.com/alexandr-kononykhin-vay/postgres/migrate/test"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

type Item struct {
	tableName struct{} `pg:"test1"`
	ID        int64    `pg:"id"`
	Field1    string   `pg:"field1"`
	Field2    int      `pg:"field2"`
}

func TestMigrate_Run(t *testing.T) {
	test.CleanDB(testDb, t)

	migrator := NewMigrator("test/migrations", os.Getenv("DSN"), WithClean("public"))
	err := migrator.Run()
	require.NoError(t, err)

	item := Item{ID: 1}
	err = testDb.Select(&item)

	require.NoError(t, err)
	require.Equal(t, "test", item.Field1)
	require.Equal(t, 123, item.Field2)
}
