package mysqlx

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// alias
type (
	DB          = sqlx.DB
	NullBool    = sql.NullBool
	NullInt64   = sql.NullInt64
	NullFloat64 = sql.NullFloat64
	NullString  = sql.NullString
	NullTime    = mysql.NullTime
	MySQLError  = mysql.MySQLError
)

var bag = &sync.Map{}

// Register dsn format -> [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
// each db should only register once.
func Register(name string, conf Conf) *DB {
	if s := os.Getenv("MYSQL_DSN_" + strings.ToUpper(name)); s != "" {
		conf.DSN = s
	}
	if s := os.Getenv("MYSQL_MAXOPEN_" + strings.ToUpper(name)); s != "" {
		d, err := strconv.Atoi(s)
		if err == nil {
			conf.MaxOpenConns = d
		}
	}
	if s := os.Getenv("MYSQL_MAXIDLE_" + strings.ToUpper(name)); s != "" {
		d, err := strconv.Atoi(s)
		if err == nil {
			conf.MaxIdleConns = d
		}
	}
	if s := os.Getenv("MYSQL_CONNMAXLIFE_" + strings.ToUpper(name)); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			conf.ConnMaxLifetime = d
		}
	}
	if s := os.Getenv("MYSQL_HOOK_" + strings.ToUpper(name)); s == "true" || s == "TRUE" || s == "True" || s == "1" {
		conf.HookEnable = true
	}

	// check if exist
	if db, ok := bag.Load(name); ok {
		return db.(*DB)
	}

	db := conf.initialize()

	// using load or store to prevent duplicate register.
	if act, loaded := bag.LoadOrStore(name, db); loaded {
		db.Close()
		return act.(*DB)
	}

	return db
}

// Client returns mysql client, mostly, we use Use() func
func Client(name string) (*DB, error) {
	v, ok := bag.Load(name)
	if !ok {
		return nil, fmt.Errorf("mysql %q not registered", name)
	}

	return v.(*DB), nil
}

// Use is helper func to get *DB
func Use(name string) *DB {
	cli, _ := Client(name)
	return cli
}

// HealthCheck ping db
func HealthCheck() error {
	errs := make(map[string]error)

	bag.Range(func(k, v interface{}) bool {
		if err := v.(*DB).Ping(); err != nil {
			errs[k.(string)] = err
		}
		return true
	})

	if len(errs) != 0 {
		return fmt.Errorf("%v", errs)
	}

	return nil
}

// Close closes all mysql conn, TODO maybe we should return close err.
func Close() error {
	bag.Range(func(k, v interface{}) bool {
		v.(*DB).Close()
		return true
	})
	return nil
}

var (
	ErrNoRows = sql.ErrNoRows
)

var (
	In                = sqlx.In
	Get               = sqlx.Get
	GetContext        = sqlx.GetContext
	Select            = sqlx.Select
	SelectContext     = sqlx.SelectContext
	Named             = sqlx.Named
	NamedExec         = sqlx.NamedExec
	NamedExecContext  = sqlx.NamedExecContext
	NamedQuery        = sqlx.NamedQuery
	NamedQueryContext = sqlx.NamedQueryContext
)

// MySQLErr try conver mysql err to *MySQLError
func MySQLErr(err error) *MySQLError {
	if err == nil {
		return nil
	}
	if e, ok := err.(*MySQLError); ok {
		return e
	}
	return nil
}

// IsNoRowsErr
func IsNoRowsErr(err error) bool {
	return err == sql.ErrNoRows
}

// IsDupErr check if mysql error is ER_DUP_ENTRY
// https://github.com/VividCortex/mysqlerr
func IsDupErr(err error) bool {
	e := MySQLErr(err)
	return e != nil && e.Number == 1062
}

type e struct {
	code uint32
	msg  string
}

func (e e) Code() uint32 {
	return e.code
}

func (e e) Message() string {
	return e.msg
}

func (e e) Error() string {
	return fmt.Sprintf("[%d]%s", e.code, e.msg)
}

// IsUnChanged checks if result.RowsAffected is 0
func IsUnChanged(result sql.Result, err error) error {
	if err != nil {
		return err
	}

	aff, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if aff == 0 {
		return e{
			code: 10304,
			msg:  "RowsAffected is 0",
		}
	}

	return nil
}
