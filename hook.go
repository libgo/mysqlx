package mysqlx

import (
	"context"
	"database/sql"
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

// Before hook will print the query with it's args and return the context with the timestamp
func (h *Hook) Before(ctx context.Context, _ string, _ ...interface{}) (context.Context, error) {
	return context.WithValue(ctx, "x-sql-begin", time.Now()), nil
}

// After hook will get the timestamp registered on the Before hook and print the elapsed time
func (h *Hook) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	startAt, ok := ctx.Value("x-sql-begin").(time.Time)
	if !ok {
		return ctx, nil
	}

	logger := logx.KVPair(map[string]interface{}{
		"span": "sql",
		"took": nanoToMs(time.Since(startAt).Nanoseconds()),
	})

	if tid, ok := ctx.Value("x-request-id").(string); ok {
		logger = logger.Trace(tid)
	}

	if logger.DebugEnabled() {
		logger.Debugf("> %s. %v", query, args)
		if soarEnable {
			out, _ := soar(query)
			logger.Debugf("%s", out)
		}
	}

	return ctx, nil
}

// convert nano to ms
func nanoToMs(ns int64) float64 {
	return math.Trunc((float64(ns)/float64(1000000)+0.5/1e2)*1e2) / 1e2
}
