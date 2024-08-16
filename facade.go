package database

import (
	"context"
	"io"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

var TxKey = new(struct{})
var LoggerKey = new(struct{})

type Client interface {
	Db() *pg.DB
	// DEPRECATED. Use Db().Begin()
	Tx() *pg.Tx

	// DEPRECATED. Use Db().Begin()
	StartTx() (*pg.Tx, error)
	// DEPRECATED. Use Db().Begin() with pg.Tx.Commit()
	Commit() error
	// DEPRECATED. Use Db().Begin() with pg.Tx.Rollback()
	Rollback() error

	Context() context.Context
	WithContext(ctx context.Context) Client
	Close() error

	Model(model ...interface{}) *orm.Query
	Select(model interface{}) error
	Insert(model ...interface{}) error
	Update(model interface{}) error
	Delete(model interface{}) error
	ForceDelete(model interface{}) error

	Exec(query interface{}, params ...interface{}) (orm.Result, error)
	ExecOne(query interface{}, params ...interface{}) (orm.Result, error)
	Query(model, query interface{}, params ...interface{}) (orm.Result, error)
	QueryOne(model, query interface{}, params ...interface{}) (orm.Result, error)

	CopyFrom(r io.Reader, query interface{}, params ...interface{}) (orm.Result, error)
	CopyTo(w io.Writer, query interface{}, params ...interface{}) (orm.Result, error)
	FormatQuery(b []byte, query string, params ...interface{}) []byte
}
