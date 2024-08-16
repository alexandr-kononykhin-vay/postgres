package database

import (
	"context"
	"io"

	pg "github.com/go-pg/pg/v10"
	orm "github.com/go-pg/pg/v10/orm"
)

type dbWrapper struct {
	ctx  context.Context
	conn *pg.DB
	tx   *pg.Tx

	wrappedProcessor func(ctx context.Context, processor func() (orm.Result, error), query string, model interface{}) (orm.Result, error)
}

func NewDbClient(conn *pg.DB, options ...Option) Client {
	dbc := &dbWrapper{conn: conn}
	for _, o := range options {
		dbc = o(dbc)
	}

	return dbc
}

func (w *dbWrapper) Db() *pg.DB {
	return w.conn
}

// DEPRECATED. Use Db().Begin()
func (w *dbWrapper) Tx() *pg.Tx {
	return w.tx
}

// DEPRECATED. Use Db().Begin()
func (w *dbWrapper) StartTx() (*pg.Tx, error) {
	tx, err := w.conn.Begin()
	if err != nil {
		return nil, err
	}

	w.tx = tx
	return tx, nil
}

// DEPRECATED. Use Db().Begin() with pg.Tx.Commit()
func (w *dbWrapper) Commit() error {
	err := w.tx.Commit()
	w.tx = nil
	return err
}

// DEPRECATED. Use Db().Begin() with pg.Tx.Rollback()
func (w *dbWrapper) Rollback() error {
	err := w.tx.Rollback()
	w.tx = nil
	return err
}

// Context ...
func (w *dbWrapper) Context() context.Context {
	if w.tx != nil {
		return w.tx.Context()
	}
	return w.conn.Context()
}

// WithContext ...
func (w *dbWrapper) WithContext(ctx context.Context) Client {
	w.ctx = ctx
	w.tx = getTxFromContext(ctx)
	return w
}

// Close ...
func (w *dbWrapper) Close() error {
	return w.conn.Close()
}

// Model ...
func (w *dbWrapper) Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(w, model...).Context(w.ctx)
}

// Select ...
func (w *dbWrapper) Select(model interface{}) error {
	if w.tx != nil {
		return w.tx.Model(model).WherePK().Select()
	}
	return w.conn.Model(model).WherePK().Select()
}

// Insert ...
func (w *dbWrapper) Insert(model ...interface{}) (err error) {
	if w.tx != nil {
		_, err = w.tx.Model(model...).Insert()
	} else {
		_, err = w.conn.Model(model...).Insert()
	}
	return err
}

// Update ...
func (w *dbWrapper) Update(model interface{}) (err error) {
	if w.tx != nil {
		_, err = w.tx.Model(model).WherePK().Update()
	} else {
		_, err = w.conn.Model(model).WherePK().Update()
	}
	return err
}

// Delete ...
func (w *dbWrapper) Delete(model interface{}) (err error) {
	if w.tx != nil {
		_, err = w.tx.Model(model).WherePK().Delete()
	} else {
		_, err = w.conn.Model(model).WherePK().Delete()
	}
	return err
}

// Exec ...
func (w *dbWrapper) Exec(query interface{}, params ...interface{}) (orm.Result, error) {
	processor := func() (orm.Result, error) {
		if w.tx != nil {
			return w.tx.Exec(query, params...)
		}
		return w.conn.Exec(query, params...)
	}

	if w.wrappedProcessor == nil {
		return processor()
	}

	return w.wrappedProcessor(w.conn.Context(), processor, w.queryString(query), nil)
}

// ExecOne ...
func (w *dbWrapper) ExecOne(query interface{}, params ...interface{}) (orm.Result, error) {
	res, err := w.Exec(query, params...)
	if err != nil {
		return nil, err
	}

	if err := w.assertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Query ...
func (w *dbWrapper) Query(model, query interface{}, params ...interface{}) (orm.Result, error) {
	processor := func() (orm.Result, error) {
		if w.tx != nil {
			return w.tx.Query(model, query, params...)
		}
		return w.conn.Query(model, query, params...)
	}

	if w.wrappedProcessor == nil {
		return processor()
	}

	return w.wrappedProcessor(w.conn.Context(), processor, w.queryString(query), model)
}

// QueryOne ...
func (w *dbWrapper) QueryOne(model, query interface{}, params ...interface{}) (orm.Result, error) {
	res, err := w.Query(model, query, params...)
	if err != nil {
		return nil, err
	}

	if err := w.assertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// CopyFrom ...
func (w *dbWrapper) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (orm.Result, error) {
	if w.tx != nil {
		return w.tx.CopyFrom(r, query, params...)
	}
	return w.conn.CopyFrom(r, query, params...)
}

// CopyTo ...
func (w *dbWrapper) CopyTo(iw io.Writer, query interface{}, params ...interface{}) (orm.Result, error) {
	if w.tx != nil {
		return w.tx.CopyTo(iw, query, params...)
	}
	return w.conn.CopyTo(iw, query, params...)
}

// FormatQuery ...
func (w *dbWrapper) FormatQuery(b []byte, query string, params ...interface{}) []byte {
	if w.tx != nil {
		return w.tx.Formatter().FormatQuery(b, query, params...)
	}
	return w.conn.Formatter().FormatQuery(b, query, params...)
}

func (w *dbWrapper) assertOneRow(affected int) error {
	if affected == 0 {
		return pg.ErrNoRows
	}
	if affected > 1 {
		return pg.ErrMultiRows
	}
	return nil
}

func (w *dbWrapper) queryString(query interface{}) string {
	switch typed := query.(type) {
	case orm.QueryAppender:
		if b, err := typed.AppendQuery(w.Formatter(), nil); err == nil {
			return string(b)
		}
	case string:
		return typed
	}
	return ""
}

// ForceDelete ...
func (w *dbWrapper) ForceDelete(values interface{}) (err error) {
	if w.tx != nil {
		_, err = w.tx.Model(values).WherePK().ForceDelete()
	} else {
		_, err = w.conn.Model(values).WherePK().ForceDelete()
	}
	return err
}

// ModelContext ...
func (w *dbWrapper) ModelContext(c context.Context, model ...interface{}) *orm.Query {
	if w.tx != nil {
		return w.tx.ModelContext(c, model...)
	}
	return w.conn.ModelContext(c, model...)
}

// ExecContext ...
func (w *dbWrapper) ExecContext(c context.Context, query interface{}, params ...interface{}) (pg.Result, error) {
	if w.tx != nil {
		return w.tx.ExecContext(c, query, params...)
	}
	return w.conn.ExecContext(c, query, params...)
}

// ExecOneContext ...
func (w *dbWrapper) ExecOneContext(c context.Context, query interface{}, params ...interface{}) (pg.Result, error) {
	if w.tx != nil {
		return w.tx.ExecOneContext(c, query, params...)
	}
	return w.conn.ExecOneContext(c, query, params...)
}

// QueryContext ...
func (w *dbWrapper) QueryContext(c context.Context, model, query interface{}, params ...interface{}) (pg.Result, error) {
	if w.tx != nil {
		return w.tx.QueryContext(c, model, query, params...)
	}
	return w.conn.QueryContext(c, model, query, params...)
}

// QueryOneContext ...
func (w *dbWrapper) QueryOneContext(c context.Context, model, query interface{}, params ...interface{}) (pg.Result, error) {
	if w.tx != nil {
		return w.tx.QueryOneContext(c, model, query, params...)
	}
	return w.conn.QueryOneContext(c, model, query, params...)
}

// Formatter ...
func (w *dbWrapper) Formatter() orm.QueryFormatter {
	if w.tx != nil {
		return w.tx.Formatter()
	}
	return w.conn.Formatter()
}

func getTxFromContext(ctx context.Context) *pg.Tx {
	tx, ok := ctx.Value(&TxKey).(*pg.Tx)
	if !ok {
		return nil
	}
	return tx
}
