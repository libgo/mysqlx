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
	NullBool    = sql.NullBool
	NullInt64   = sql.NullInt64
	NullFloat64 = sql.NullFloat64
	NullString  = sql.NullString
	NullTime    = mysql.NullTime
)

// var store = safemap.New()

var bag = sync.Map{}

// Register dsn format -> [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
// each db should only register once.
func Register(name string, conf Conf) {
	// override if exist in env
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
	if s := os.Getenv("MYSQL_HOOKOFF_" + strings.ToUpper(name)); s == "true" || s == "TRUE" || s == "True" || s == "1" {
		conf.HookDisable = true
	}

	db := conf.initialize()
	bag.LoadOrStore(name, db) // using load or store to prevent duplicate register.
}

// Client returns mysql client, mostly, we use DB() func
func Client(name string) (*sqlx.DB, error) {
	v, ok := bag.Load(name)
	if !ok {
		return nil, fmt.Errorf("mysql %q not registered", name)
	}

	return v.(*sqlx.DB), nil
}

// DB is helper func to get *sqlx.DB
func DB(name string) *sqlx.DB {
	cli, _ := Client(name)
	return cli
}

// HealthCheck ping db
func HealthCheck() error {
	errs := make(map[string]error)

	bag.Range(func(k, v interface{}) bool {
		if err := v.(*sqlx.DB).Ping(); err != nil {
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
		v.(*sqlx.DB).Close()
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

// MySQLErr try conver mysql err to *mysql.MySQLError
func MySQLErr(err error) *mysql.MySQLError {
	if err == nil {
		return nil
	}
	if e, ok := err.(*mysql.MySQLError); ok {
		return e
	}
	return nil
}

// IsNoRowsErr
func IsNoRowsErr(err error) bool {
	return err == sql.ErrNoRows
}

const (
	ER_DUP_ENTRY = 1062
)

// IsDupErr check if mysql error is ER_DUP_ENTRY
// https://github.com/VividCortex/mysqlerr
func IsDupErr(err error) bool {
	e := MySQLErr(err)
	return e != nil && e.Number == ER_DUP_ENTRY
}

var ErrAff = &e{
	code: 1404,
	msg:  "RowsAffected is 0",
}

type e struct {
	code uint32
	msg  string
}

func (e *e) Code() uint32 {
	return e.code
}

func (e *e) Message() string {
	return e.msg
}

func (e *e) Error() string {
	return fmt.Sprintf("[%d]%s", e.code, e.msg)
}

// IsChanged checks if result.RowsAffected is 0
func IsChanged(result sql.Result, err error) error {
	if err != nil {
		return err
	}

	aff, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if aff == 0 {
		return ErrAff
	}

	return nil
}
