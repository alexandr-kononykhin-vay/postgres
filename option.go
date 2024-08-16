package database

import (
	"go.uber.org/zap"
	"time"
)

type Option func(w *dbWrapper) *dbWrapper

func WithLogger(logger *zap.Logger, duration time.Duration) Option {
	logger.Info("long db query logging enabled", zap.Duration("over", duration))

	return func(w *dbWrapper) *dbWrapper {
		dbLogger := newDBLogger(logger, duration)
		w.Db().AddQueryHook(dbLogger)
		return w
	}
}
