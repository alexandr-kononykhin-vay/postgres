package database

import (
	"context"
	"reflect"

	pg "github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

func ConnectWithDSN(appName, dsn string, options ...Option) (Client, error) {
	cfg, err := pg.ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	client := Connect(appName, cfg, options...)
	if _, err := client.ExecOne("select 1"); err != nil {
		return nil, err
	}
	return client, nil
}

func Connect(AppName string, cfg *pg.Options, options ...Option) Client {
	if cfg.OnConnect == nil {
		cfg.OnConnect = onConnect(AppName)
	}

	return NewDbClient(pg.Connect(cfg), options...)
}

// GetTableName returns table name by model
func GetTableName(model interface{}) string {
	if t := orm.GetTable(reflect.TypeOf(model)); t != nil {
		return string(t.SQLName)
	}
	return ""
}

// set client timezone to UTC
func onConnect(appName string) func(ctx context.Context, conn *pg.Conn) error {
	return func(ctx context.Context, conn *pg.Conn) error {
		_, _ = conn.ExecContext(ctx, "set timezone='UTC'")
		_, _ = conn.ExecContext(ctx, "set application_name=?", appName)
		return nil
	}
}
