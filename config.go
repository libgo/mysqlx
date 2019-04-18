package mysqlx

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// Conf is mysql driver and conn related config.
type Conf struct {
	DSN             string              // using env MYSQL_DSN_ can override
	MaxOpenConns    int                 // ==0, set to 512, <0 no limit on the number of open connections
	MaxIdleConns    int                 // ==0, set to 64, <0 no idle connections are retained
	ConnMaxLifetime time.Duration       // ==0, set to 15m, <0 connections are reused forever
	MapperFunc      func(string) string // struct field name convert
	HookDisable     bool
}

func (c *Conf) initialize() *sqlx.DB {
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 512
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 64
	}
	if c.ConnMaxLifetime == 0 {
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

	db.SetMaxOpenConns(c.MaxOpenConns)
	db.SetMaxIdleConns(c.MaxIdleConns)
	db.SetConnMaxLifetime(c.ConnMaxLifetime)

	db.MapperFunc(c.MapperFunc)

	return db
}
