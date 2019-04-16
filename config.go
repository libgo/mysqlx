package mysqlx

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// Conf is mysql driver and conn related config.
type Conf struct {
	DSN             string
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
	MapperFunc      func(string) string // struct field name convert
	HookDisable     bool
}

func (c *Conf) initialize() *sqlx.DB {
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = 512
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = 64
	}
	if c.ConnMaxLifetime <= 0 {
		c.ConnMaxLifetime = time.Minute * 15
	}
	if c.MapperFunc == nil {
		c.MapperFunc = snakecase
	}

	var db *sqlx.DB
	var err error
	if c.HookDisable {
		db, err = sqlx.Open("mysql", c.DSN)
	} else {
		// should register related driver in init func
		db, err = sqlx.Open(driverName, c.DSN)
	}
	if err != nil {
		panic(err)
	}

	db.SetMaxIdleConns(c.MaxIdleConns)
	db.SetMaxOpenConns(c.MaxOpenConns)
	db.SetConnMaxLifetime(c.ConnMaxLifetime)
	db.MapperFunc(c.MapperFunc)

	return db
}
