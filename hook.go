package mysqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"math"
	"time"

	"github.com/gchaincl/sqlhooks"
	"github.com/go-sql-driver/mysql"
	"github.com/libgo/logx"
)

const (
	driverName = "mysql_with_hook"
)

func init() {
	sql.Register(driverName, sqlhooks.Wrap(&mysql.MySQLDriver{}, &Hook{}))
}

type Hook struct {
}

type sqlTimer struct{}

// Before hook will print the query with it's args and return the context with the timestamp
func (h *Hook) Before(ctx context.Context, _ string, _ ...interface{}) (context.Context, error) {
	return context.WithValue(ctx, sqlTimer{}, time.Now()), nil
}

// After hook will get the timestamp registered on the Before hook and print the elapsed time
func (h *Hook) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	logger := logx.FromContext(ctx)

	if logger.DebugEnabled() {
		if startAt, ok := ctx.Value(sqlTimer{}).(time.Time); ok {
			logger.KV("span", "sql", "took", nanoToMs(time.Since(startAt).Nanoseconds())).Debugf("> %s. %v", query, args)
		}
	}

	return ctx, nil
}

func (h *Hook) OnError(ctx context.Context, err error, query string, args ...interface{}) error {
	// skip
	if err == driver.ErrSkip || err == driver.ErrBadConn {
		return err
	}

	if startAt, ok := ctx.Value(sqlTimer{}).(time.Time); ok {
		logger := logx.FromContext(ctx)
		logger.KV("span", "sql", "took", nanoToMs(time.Since(startAt).Nanoseconds())).Errorf("> %s. %v", query, args)
	}

	return err
}

// convert nano to ms
func nanoToMs(ns int64) float64 {
	return math.Trunc((float64(ns)/float64(1000000)+0.5/1e2)*1e2) / 1e2
}
