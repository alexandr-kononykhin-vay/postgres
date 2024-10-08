//go:build !ci
// +build !ci

package dao

import (
	"context"
	"errors"
	"github.com/go-pg/pg/v10"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/alexandr-kononykhin-vay/postgres/repository/filter"

	pkgerr "github.com/alexandr-kononykhin-vay/postgres/errors"
	"github.com/alexandr-kononykhin-vay/postgres/repository/dao/test"
	"github.com/alexandr-kononykhin-vay/postgres/repository/opt"

	"github.com/stretchr/testify/assert"
)

func TestRepository_WithTX(t *testing.T) {
	test.CleanDB(testDb, t)
	repo := New(testDb)

	t.Run("Success", func(t *testing.T) {
		err := repo.WithTX(context.Background(), func(ctx context.Context) error {
			return repo.Insert(ctx, &Agent{ID: 111, Name: "test-tx"})
		})
		assert.Nil(t, err)

		got := &Agent{ID: 111}
		err = testDb.Select(got)

		assert.Nil(t, err)
		assert.Equal(t, "test-tx", got.Name)
	})

	t.Run("Error", func(t *testing.T) {
		ctx := context.Background()
		err := repo.WithTX(ctx, func(ctx context.Context) error {
			err := repo.Insert(ctx, &Agent{ID: 222, Name: "test-tx"})
			assert.Nil(t, err)
			return pkgerr.NewInternalError(errors.New("error"))
		})
		assert.NotNil(t, err)

		got := &Agent{ID: 222}
		err = testDb.Select(got)
		assert.Equal(t, pg.ErrNoRows, err, "Transaction doesn't work")
	})
}

func TestRepository_WithTX_ContextDone(t *testing.T) {
	test.CleanDB(testDb, t)
	repo := New(testDb)

	t.Run("Main context", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), 300*time.Millisecond)
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			time.Sleep(100 * time.Millisecond)
			return repo.WithTX(gCtx, func(ctx context.Context) error {
				return repo.Insert(ctx, &Agent{ID: 111, Name: "test-tx1"})
			})
		})
		g.Go(func() error {
			time.Sleep(400 * time.Millisecond)
			return repo.WithTX(gCtx, func(ctx context.Context) error {
				return repo.Insert(ctx, &Agent{ID: 222, Name: "test-tx2"})
			})
		})

		err := g.Wait()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "context deadline exceeded")

		got1 := &Agent{ID: 111}
		err = testDb.Select(got1)
		assert.NoError(t, err)
		assert.Equal(t, "test-tx1", got1.Name)

		got2 := &Agent{ID: 222}
		err = testDb.Select(got2)
		assert.Equal(t, pg.ErrNoRows, err, "Transaction doesn't work")
	})

	t.Run("Error group context", func(t *testing.T) {
		g, gCtx := errgroup.WithContext(context.Background())
		g.Go(func() error {
			time.Sleep(100 * time.Millisecond)
			return repo.WithTX(gCtx, func(ctx context.Context) error {
				return repo.Insert(ctx, &Agent{ID: 333, Name: "test-tx"})
			})
		})
		g.Go(func() error {
			return repo.WithTX(gCtx, func(ctx context.Context) error {
				return errors.New("some err")
			})
		})

		err := g.Wait()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "some err")

		got := &Agent{ID: 333}
		err = testDb.Select(got)
		assert.Equal(t, pg.ErrNoRows, err, "Transaction doesn't work")
	})
}

func TestRepository_FindOne_FindList(t *testing.T) {
	test.CleanDB(testDb, t)
	repo := New(testDb)

	err := testDb.Insert(
		&Agent{ID: 1, Name: "111", INN: "111777111", Meta: `{"a": "valueA", "b": "valueB", "c": "valueC"}`},
		&Agent{ID: 2, Name: "222", INN: "222222222", Meta: `{"a": {"d": "valueD"}}`},
		&Agent{ID: 3, Name: "333", INN: "333777333"},
	)
	assert.Nil(t, err)

	t.Run("FindOne", func(t *testing.T) {
		t.Run("Eq", func(t *testing.T) {
			var rec1, rec2 Agent
			err1 := repo.FindOne(context.Background(), &rec1, opt.List(opt.Eq("id", 1)))
			err2 := repo.FindOne(context.Background(), &rec2, opt.List(opt.Eq("name", "333")))

			assert.Nil(t, err1)
			assert.Nil(t, err2)
			assert.Equal(t, "111", rec1.Name)
			assert.Equal(t, int64(3), rec2.ID)
		})

		t.Run("In", func(t *testing.T) {
			var rec Agent
			err := repo.FindOne(context.Background(), &rec, opt.List(opt.In("id", []int64{1, 11, 111})))

			assert.Nil(t, err)
			assert.Equal(t, "111", rec.Name)
		})

		t.Run("MayIn", func(t *testing.T) {
			var rec Agent
			err := repo.FindOne(context.Background(), &rec, opt.List(opt.MayIn("id", []int64{}), opt.Desc("id")))

			assert.Nil(t, err)
			assert.Equal(t, "333", rec.Name)
			assert.Equal(t, int64(3), rec.ID)
		})

		t.Run("With empty result", func(t *testing.T) {
			err := repo.FindOne(context.Background(), &Agent{}, opt.List(opt.Eq("id", 123)))
			assert.True(t, pkgerr.IsNotFound(err))
		})
	})

	t.Run("FindList", func(t *testing.T) {
		t.Run("Or", func(t *testing.T) {
			var recs []*Agent
			err := repo.FindList(context.Background(), &recs, opt.List(
				opt.Or(opt.Eq("id", 1), opt.Eq("id", 3)), opt.Desc("id"),
			))

			assert.Nil(t, err)
			assert.Equal(t, 2, len(recs))
			assert.Equal(t, int64(3), recs[0].ID)
			assert.Equal(t, "333", recs[0].Name)
			assert.Equal(t, int64(1), recs[1].ID)
			assert.Equal(t, "111", recs[1].Name)
		})

		t.Run("And", func(t *testing.T) {
			var recs []*Agent
			err := repo.FindList(context.Background(), &recs, opt.List(
				opt.And(opt.Eq("id", 1), opt.Eq("inn", "111777111")),
			))

			assert.Nil(t, err)
			assert.Equal(t, 1, len(recs))
			assert.Equal(t, int64(1), recs[0].ID)
			assert.Equal(t, "111", recs[0].Name)
		})

		t.Run("Or + And", func(t *testing.T) {
			var recs []*Agent
			err := repo.FindList(context.Background(), &recs, opt.List(opt.Or(
				opt.And(opt.Eq("id", 1), opt.Eq("inn", "111777111")),
				opt.And(opt.Eq("id", 2), opt.Eq("name", "222")),
				opt.And(opt.Eq("id", 3), opt.Eq("inn", "33")),
			)))

			assert.Nil(t, err)
			assert.Equal(t, 2, len(recs))
			assert.Equal(t, int64(1), recs[0].ID)
			assert.Equal(t, "111", recs[0].Name)
			assert.Equal(t, int64(2), recs[1].ID)
			assert.Equal(t, "222", recs[1].Name)
		})

		t.Run("And + Or", func(t *testing.T) {
			var recs []*Agent
			err := repo.FindList(context.Background(), &recs, opt.List(opt.And(
				opt.Or(opt.Eq("id", 2), opt.Eq("id", 10)),
				opt.Or(opt.Eq("name", "222"), opt.Eq("name", "10000222")),
			)))

			assert.Nil(t, err)
			assert.Equal(t, 1, len(recs))
			assert.Equal(t, int64(2), recs[0].ID)
			assert.Equal(t, "222", recs[0].Name)
		})

		t.Run("Contains", func(t *testing.T) {
			var recs []*Agent
			err := repo.FindList(context.Background(), &recs, opt.List(opt.Contains("inn", "777"), opt.Desc("id")))

			assert.Nil(t, err)
			assert.Equal(t, int64(3), recs[0].ID)
			assert.Equal(t, "333", recs[0].Name)
			assert.Equal(t, int64(1), recs[1].ID)
			assert.Equal(t, "111", recs[1].Name)
		})

		t.Run("JsonEq", func(t *testing.T) {
			var recs []*Agent
			err := repo.FindList(context.Background(), &recs, opt.List(
				opt.JsonEq("meta", "valueD", "a", "d")))

			assert.Nil(t, err)
			assert.Equal(t, 1, len(recs))
			assert.Equal(t, int64(2), recs[0].ID)
		})

		t.Run("JsonContains", func(t *testing.T) {
			var recs []*Agent
			err := repo.FindList(context.Background(), &recs, opt.List(
				opt.JsonContains("meta",
					filter.JsonPath{Path: "a", Value: "valueA"},
					filter.JsonPath{Path: "c", Value: "valueC"}),
			))

			assert.Nil(t, err)
			assert.Equal(t, 1, len(recs))
			assert.Equal(t, int64(1), recs[0].ID)
		})

		t.Run("JsonContainsValue", func(t *testing.T) {
			var recs []*Agent
			err := repo.FindList(context.Background(), &recs, opt.List(
				opt.JsonContainsValue("meta", "val", "a", "d"),
			))

			assert.Nil(t, err)
			assert.Equal(t, 1, len(recs))
			assert.Equal(t, int64(2), recs[0].ID)
		})
	})
}

func TestRepository_Insert(t *testing.T) {
	test.CleanDB(testDb, t)
	repo := New(testDb)

	t.Run("Agent", func(t *testing.T) {
		rec := &Agent{Name: "insert-test", State: AgentStateRegistered}
		err := repo.Insert(context.Background(), rec)
		assert.Nil(t, err)
		assert.True(t, rec.ID > 0)

		got := &Agent{ID: rec.ID}
		err = testDb.Select(got)

		assert.Nil(t, err)
		assert.Equal(t, rec.Name, got.Name)
		assert.Equal(t, AgentStateRegistered, got.State)
		assert.Equal(t, AgentStateRegistered, rec.State)
	})
}

func TestRepository_Update(t *testing.T) {
	test.CleanDB(testDb, t)
	repo := New(testDb)

	ts := time.Now().Add(-time.Hour)
	agent := &Agent{ID: 1, Name: "111", INN: "111", Created: ts, Updated: ts}
	err := testDb.Insert(agent)
	assert.Nil(t, err)

	agent.Name = "222"
	agent.INN = "222"
	err = repo.Update(context.Background(), agent, "name")
	assert.Nil(t, err)

	got := &Agent{ID: agent.ID}
	err = testDb.Select(got)

	assert.Nil(t, err)
	assert.Equal(t, "222", got.Name)
	assert.Equal(t, "111", got.INN)
	assert.True(t, got.Updated.In(time.UTC).Unix() >= ts.In(time.UTC).Unix(), "got: %v >= %v", got.Updated.In(time.UTC), ts.In(time.UTC))
	assert.True(t, agent.Updated.In(time.UTC).Unix() >= ts.In(time.UTC).Unix(), "agent: %v >= %v", agent.Updated.In(time.UTC), ts.In(time.UTC))
}

func TestRepository_UpdateWhere(t *testing.T) {
	test.CleanDB(testDb, t)
	repo := New(testDb)

	ts := time.Now().Add(-time.Hour)
	agentList := []*Agent{
		{ID: 1, Name: "111", INN: "333", Created: ts, Updated: ts},
		{ID: 2, Name: "111", INN: "333", Created: ts, Updated: ts},
		{ID: 3, Name: "222", INN: "333", Created: ts, Updated: ts},
	}
	for _, agent := range agentList {
		err := testDb.Insert(agent)
		assert.Nil(t, err)
	}

	updateNameValue := "111"

	err := repo.UpdateWhere(context.Background(), &Agent{}, opt.List(opt.Eq("name", updateNameValue)), "inn", "222")
	assert.Nil(t, err)

	var gotList []*Agent
	err = testDb.Model(&gotList).Select()
	assert.Nil(t, err)
	for _, got := range gotList {
		if got.Name == updateNameValue {
			assert.Equal(t, "222", got.INN)
			assert.True(t, got.Updated.Unix() > ts.Unix())
		} else {
			assert.Equal(t, "333", got.INN)
		}
	}
}

func TestRepository_SelectValue(t *testing.T) {
	test.CleanDB(testDb, t)

	for _, val := range []string{"", "one"} {
		t.Run("Test on "+val, func(t *testing.T) {
			entity := Agent{ServiceLevel: &val}
			err := testDb.Insert(&entity)
			assert.Nil(t, err)
			gotEntity := Agent{ID: entity.ID}
			err = testDb.Model(&gotEntity).WherePK().Select()
			assert.Nil(t, err)
			assert.NotNil(t, gotEntity.ServiceLevel)
			assert.Equal(t, val, *gotEntity.ServiceLevel)
		})
	}

	t.Run("Test on NULL", func(t *testing.T) {
		entity := Agent{}
		err := testDb.Insert(&entity)
		assert.Nil(t, err)
		gotEntity := Agent{ID: entity.ID}
		err = testDb.Model(&gotEntity).WherePK().Select()
		assert.Nil(t, err)
		assert.Nil(t, gotEntity.ServiceLevel)
	})
}

func TestRepository_SoftDelete(t *testing.T) {
	test.CleanDB(testDb, t)
	rep := New(testDb)

	t.Run("Agent", func(t *testing.T) {
		rec := &Agent{ID: 111}

		err := testDb.Insert(rec)
		assert.Nil(t, err)

		err = rep.SoftDelete(context.Background(), rec)
		assert.Nil(t, err)

		got := &Agent{ID: 111}
		err = testDb.Select(got)

		assert.Nil(t, err)
		assert.True(t, !got.Deleted.IsZero())
		assert.Equal(t, rec.Deleted.Unix(), got.Deleted.Unix())
	})
}

func TestRepository_HardDelete(t *testing.T) {
	test.CleanDB(testDb, t)
	rep := New(testDb)

	rec := &Agent{ID: 111}
	err := testDb.Insert(rec)
	assert.Nil(t, err)

	err = rep.HardDelete(context.Background(), rec)
	assert.Nil(t, err)

	got := &Agent{ID: 111}
	err = testDb.Select(got)

	assert.Equal(t, pg.ErrNoRows, err)
}

func TestRepository_UpsertSlice(t *testing.T) {
	test.CleanDB(testDb, t)

	name11 := "test11"
	name12 := "test12"
	rec := []*Agent{{ID: 111, Name: name11}, {ID: 222, Name: name12}}
	rep := New(testDb)

	err := rep.Upsert(context.Background(), rec, []string{"id"}, "name")
	assert.Nil(t, err)

	got := &Agent{ID: 111}
	err = testDb.Select(got)
	assert.NoError(t, err)
	assert.Equal(t, name11, got.Name)

	got = &Agent{ID: 222}
	err = testDb.Select(got)
	assert.NoError(t, err)
	assert.Equal(t, name12, got.Name)

	name21 := "test21"
	name22 := "test22"
	rec = []*Agent{{ID: 111, Name: name21}, {ID: 222, Name: name22}}
	err = rep.Upsert(context.Background(), rec, []string{"id"}, "name")
	assert.Nil(t, err)

	got = &Agent{ID: 111}
	err = testDb.Select(got)
	assert.NoError(t, err)
	assert.Equal(t, name21, got.Name)

	got = &Agent{ID: 222}
	err = testDb.Select(got)
	assert.NoError(t, err)
	assert.Equal(t, name22, got.Name)
}

func TestRepository_UpsertSingle(t *testing.T) {
	test.CleanDB(testDb, t)
	rep := New(testDb)

	name11 := "test11"
	rec := &Agent{ID: 111, Name: name11}

	err := rep.Upsert(context.Background(), rec, []string{"id"}, "name")
	assert.Nil(t, err)

	got := &Agent{ID: 111}
	err = testDb.Select(got)
	assert.NoError(t, err)
	assert.Equal(t, name11, got.Name)

	name12 := "test12"
	rec = &Agent{ID: 111, Name: name12}
	err = rep.Upsert(context.Background(), rec, []string{"id"}, "name")
	assert.Nil(t, err)

	got = &Agent{ID: 111}
	err = testDb.Select(got)
	assert.NoError(t, err)
	assert.Equal(t, name12, got.Name)
}

func TestRepository_UpsertSliceWithDoubles(t *testing.T) {
	test.CleanDB(testDb, t)
	rep := New(testDb)

	name11 := "test11"
	name12 := "test12"
	rec := []Agent{{ID: 111, Name: name11}, {ID: 111, Name: name12}}

	err := rep.Upsert(context.Background(), rec, []string{"id"}, "name")
	assert.Nil(t, err)

	got := &Agent{ID: 111}
	err = testDb.Select(got)
	assert.NoError(t, err)
	assert.Equal(t, name12, got.Name)
}
