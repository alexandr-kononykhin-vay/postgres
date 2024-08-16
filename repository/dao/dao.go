package dao

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	db "github.com/alexandr-kononykhin-vay/postgres"
	pkgerr "github.com/alexandr-kononykhin-vay/postgres/errors"
	"github.com/alexandr-kononykhin-vay/postgres/repository/opt"
	pg "github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

type DeletedSetter interface {
	SetDeleted(time.Time)
}

type DAO struct {
	db           db.Client
	updatedField string
	deletedField string
}

func New(db db.Client) *DAO {
	return &DAO{
		db:           db,
		updatedField: "updated",
		deletedField: "deleted",
	}
}

func (r *DAO) SetUpdatedField(fieldName string) {
	if fieldName == "" {
		return
	}
	r.updatedField = fieldName
}

func (r *DAO) SetDeletedField(fieldName string) {
	if fieldName == "" {
		return
	}
	r.deletedField = fieldName
}

func (r *DAO) DB() db.Client {
	return r.db
}

func (r *DAO) Ping(ctx context.Context) error {
	_, err := r.db.WithContext(ctx).Exec("SELECT 1")
	return err
}

// WithTX executes passed function within transaction
func (r *DAO) WithTX(ctx context.Context, fn func(context.Context) error) error {
	if r.db.Tx() != nil {
		return fn(ctx)
	}

	tx, err := r.db.WithContext(ctx).StartTx()
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	if err := fn(newTxContext(ctx, tx)); err != nil || ctx.Err() != nil {
		if rollbackErr := r.db.Rollback(); rollbackErr != nil {
			// TODO: get logger from context
			log.Println(fmt.Sprintf("failed to rollback transaction: %s", rollbackErr.Error()))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}

	if err := r.db.Commit(); err != nil {
		return pkgerr.Convert(ctx, err)
	}
	return nil
}

// FindOne selects the only record from database according to opts
func (r *DAO) FindOne(ctx context.Context, receiver interface{}, opts []opt.FnOpt) error {
	err := r.db.WithContext(ctx).Model(receiver).Apply(opt.Apply(opts...)).First()
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// FindList selects all records from database according to opts
func (r *DAO) FindList(ctx context.Context, receiver interface{}, opts []opt.FnOpt) error {
	err := r.db.WithContext(ctx).Model(receiver).Apply(opt.Apply(opts...)).Select()
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// FindListWithTotal selects all records and total count of records from database according to opts
func (r *DAO) FindListWithTotal(ctx context.Context, receiver interface{}, opts []opt.FnOpt) (total int, err error) {
	total, err = r.db.WithContext(ctx).Model(receiver).Apply(opt.Apply(opts...)).SelectAndCount()
	if err != nil {
		return 0, pkgerr.Convert(ctx, err)
	}

	return total, nil
}

// GetTotal get total count of records from database according to opts
func (r *DAO) GetTotal(ctx context.Context, receiver interface{}, opts []opt.FnOpt) (int, error) {
	total, err := r.db.WithContext(ctx).Model(receiver).Apply(opt.Apply(opts...)).Count()
	if err != nil {
		return 0, pkgerr.Convert(ctx, err)
	}

	return total, nil
}

// Update updates a record
func (r *DAO) Update(ctx context.Context, rec interface{}, columns ...string) error {
	columns = append(columns, r.updatedField)
	q := r.db.WithContext(ctx).Model(rec).Column(columns...)
	// Slice not require additional filter
	if reflect.ValueOf(rec).Elem().Type().Kind() != reflect.Slice {
		q.WherePK()
	}
	_, err := q.Update()
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// UpdateWhere updates a record with condition
func (r *DAO) UpdateWhere(ctx context.Context, rec interface{}, opts []opt.FnOpt, setFieldValuePairs ...interface{}) error {
	if len(setFieldValuePairs)&1 != 0 {
		return pkgerr.NewInternalError(fmt.Errorf("UpdateWhere: setFieldValuePairs must be even, got %d", len(setFieldValuePairs)))
	}
	setFieldValuePairs = append(setFieldValuePairs, r.updatedField, time.Now())
	q := r.db.WithContext(ctx).Model(rec).Apply(opt.Apply(opts...))
	for i := 0; i < len(setFieldValuePairs); i += 2 {
		column, ok := setFieldValuePairs[i].(string)
		if !ok {
			return pkgerr.NewInternalError(fmt.Errorf("UpdateWhere: field must be string, got %T (%v)", setFieldValuePairs[i], setFieldValuePairs[i]))
		}
		q.Set(column+" = ?", setFieldValuePairs[i+1])
	}
	_, err := q.Update()
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// UpdateWithReturning updates a record
func (r *DAO) UpdateWithReturning(ctx context.Context, rec interface{}, columns ...string) error {
	columns = append(columns, r.updatedField)
	_, err := r.db.WithContext(ctx).Model(rec).Column(columns...).WherePK().Returning("*").Update()
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// Insert creates a new record
func (r *DAO) Insert(ctx context.Context, rec ...interface{}) error {
	err := r.db.WithContext(ctx).Insert(rec...)
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// SoftDelete marks record as deleted
func (r *DAO) SoftDelete(ctx context.Context, rec DeletedSetter) error {
	rec.SetDeleted(time.Now())
	err := r.Update(ctx, rec, r.deletedField)
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// HardDelete removes record from database
func (r *DAO) HardDelete(ctx context.Context, rec interface{}) error {
	err := r.db.WithContext(ctx).Delete(rec)
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// HardDeleteWhere removes record from database
func (r *DAO) HardDeleteWhere(ctx context.Context, rec interface{}, opts []opt.FnOpt) error {
	_, err := r.db.WithContext(ctx).Model(rec).Apply(opt.ApplyFilter(opts...)).Delete()
	if err != nil {
		return pkgerr.Convert(ctx, err)
	}

	return nil
}

// Upsert inserts recs, on conflict update columns
func (r *DAO) Upsert(ctx context.Context, recs interface{}, keys []string, columns ...string) error {
	if len(keys) == 0 {
		return pkgerr.NewBadRequestError(errors.New("keys cannot be empty"))
	}

	goNames := make([]string, 0, len(keys))
	if t := orm.GetTable(getType(recs)); t != nil {
		for _, key := range keys {
			goNames = append(goNames, t.FieldsMap[key].GoName)
		}
	}

	var models []interface{}
	k := reflect.TypeOf(recs).Kind()
	if k == reflect.Slice {
		models = GetUniqueModels(recs, func(model interface{}) string {
			values := make([]string, 0, len(goNames))
			for _, key := range goNames {
				values = append(values, fmt.Sprint(reflect.ValueOf(model).Elem().FieldByName(key)))
			}
			return strings.Join(values, "_")
		})
	} else if k == reflect.Ptr {
		models = []interface{}{recs}
	} else {
		return pkgerr.NewBadRequestError(errors.New("recs must be slice or pointer to struct"))
	}

	if len(models) == 0 {
		return pkgerr.NewBadRequestError(errors.New("models cannot be empty"))
	}

	q := r.db.WithContext(ctx).Model(&models).OnConflict("(" + strings.Join(keys, ",") + ") DO UPDATE")

	for _, column := range columns {
		q = q.Set(column + " = EXCLUDED." + column)
	}

	_, err := q.Insert()
	return err
}

func getType(models interface{}) reflect.Type {
	var m interface{}

	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(models)
		if s.Index(0).Kind() == reflect.Ptr {
			m = s.Index(0).Elem().Interface()
		} else {
			m = s.Index(0).Interface()
		}

	case reflect.Ptr:
		m = reflect.ValueOf(models).Elem().Interface()

	case reflect.Struct:
		m = reflect.ValueOf(models).Interface()

	}

	return reflect.TypeOf(m)
}

// GetUniqueModels - make models unique according to key returned by f
// if two models have the same key, the last one takes precedence
func GetUniqueModels(models interface{}, f func(model interface{}) string) []interface{} {
	rows := make(map[string]interface{})

	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(models)

		for i := 0; i < s.Len(); i++ {
			var model interface{}
			if s.Index(i).Kind() == reflect.Ptr {
				model = s.Index(i).Interface()
			} else {
				model = s.Index(i).Addr().Interface()
			}

			rows[f(model)] = model
		}
	}

	unique := make([]interface{}, 0, len(rows))
	for i := range rows {
		unique = append(unique, rows[i])
	}

	return unique
}

func newTxContext(ctx context.Context, tx *pg.Tx) context.Context {
	return context.WithValue(ctx, &db.TxKey, tx)
}
