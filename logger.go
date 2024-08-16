package database

import (
	"context"
	"fmt"
	"time"

	"github.com/go-pg/pg/v10"
	"go.uber.org/zap"
)

const queryStartTime = "StartTime"

type DBLogger interface {
	pg.QueryHook
}

type dbLogger struct {
	logger   *zap.Logger
	duration time.Duration
}

func newDBLogger(logger *zap.Logger, duration time.Duration) DBLogger {
	return &dbLogger{
		logger:   logger,
		duration: duration,
	}
}

func (d *dbLogger) BeforeQuery(ctx context.Context, event *pg.QueryEvent) (context.Context, error) {
	if event.Stash == nil {
		event.Stash = make(map[interface{}]interface{})
	}
	event.Stash[queryStartTime] = time.Now()
	return ctx, nil
}

func (d *dbLogger) AfterQuery(ctx context.Context, event *pg.QueryEvent) error {
	query, err := event.FormattedQuery()
	if err == nil {
		var duration time.Duration
		if event.Stash != nil {
			if v, ok := event.Stash[queryStartTime]; ok {
				duration = time.Since(v.(time.Time))
			}
		}
		logLevel := zap.InfoLevel
		if d.duration != 0 {
			if d.duration > duration {
				return nil
			}
			logLevel = zap.WarnLevel
		}
		txt := "query: " + string(query)
		if duration != 0 {
			txt += fmt.Sprintf(" [%d ms]", duration.Nanoseconds()/1000000)
		}
		if event.Err != nil {
			txt += "\nerror: " + event.Err.Error()
		}

		d.logger.Log(logLevel, txt)
	}
	return nil
}
