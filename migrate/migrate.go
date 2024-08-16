package migrate

import (
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"strings"
)

const driverName = "postgres"

type Migrator struct {
	path string
	dsn  string

	cleanScheme []string
	logger      *zap.Logger
}

func NewMigrator(path, dsn string, options ...OptionFn) *Migrator {
	m := &Migrator{
		path:   fmt.Sprintf("file://%s", strings.TrimPrefix(strings.TrimPrefix(path, "."), "/")),
		dsn:    dsn,
		logger: zap.NewNop(),
	}

	for _, opt := range options {
		opt(m)
	}
	return m
}

func (m *Migrator) Run() error {
	db, err := sql.Open(driverName, m.dsn)
	if err != nil {
		m.logger.Error("failed to connect database", zap.Error(err))
		return err
	}
	defer db.Close()

	if len(m.cleanScheme) > 0 {
		for _, scheme := range m.cleanScheme {
			if err := m.cleanDatabase(db, scheme); err != nil {
				return err
			}
		}
	}

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	migration, err := migrate.NewWithDatabaseInstance(m.path, driverName, driver)
	if err != nil {
		return err
	}

	beforeVersion, dirty, err := migration.Version()
	if err != nil && beforeVersion != 0 {
		return err
	}

	m.logger.Info("migration started", zap.Uint("version", beforeVersion))

	if dirty {
		m.logger.Warn("previous migration failed")
	}

	err = migration.Up()

	if err != nil && err != migrate.ErrNoChange {
		return err
	} else if err == migrate.ErrNoChange {
		m.logger.Info("no new database changes")
	}

	afterVersion, dirty, err := migration.Version()
	if err != nil && beforeVersion != 0 {
		return err
	}

	m.logger.Info("migration done", zap.Uint("version", afterVersion))

	if dirty {
		m.logger.Warn("previous migration failed")
	}

	return nil
}

// Clean database public scheme
func (m *Migrator) cleanDatabase(db *sql.DB, schema string) error {
	m.logger.Info("clean schema", zap.String("schema", schema))
	_, err := db.Query("DROP SCHEMA " + schema + " CASCADE")
	if err != nil {
		return err
	}
	_, err = db.Query("CREATE SCHEMA " + schema)
	if err != nil {
		return err
	}
	return nil
}
