package starling

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
)

var _ Executer = (*sql.DB)(nil)
var _ Executer = (*sql.Tx)(nil)

type Executer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

var _ ExecContexter = (*sql.DB)(nil)
var _ ExecContexter = (*sql.Tx)(nil)

type ExecContexter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

var _ Querier = (*sql.DB)(nil)
var _ Querier = (*sql.Tx)(nil)

type Querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

var _ QueryContexter = (*sql.DB)(nil)
var _ QueryContexter = (*sql.Tx)(nil)

type QueryContexter interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

var _ QueryRower = (*sql.DB)(nil)
var _ QueryRower = (*sql.Tx)(nil)

type QueryRower interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

var _ QueryRowContexter = (*sql.DB)(nil)
var _ QueryRowContexter = (*sql.Tx)(nil)

type QueryRowContexter interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type DB interface {
	Executer
	ExecContexter
	Querier
	QueryContexter
	QueryRower
	QueryRowContexter
}

func Transaction(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "cannot begin transaction")
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback() // nolint:gosec,errcheck
			panic(r)
		}

		if err != nil {
			tx.Rollback() // nolint:gosec,errcheck
			return
		}

		err = tx.Commit()
	}()

	if err := fn(tx); err != nil {
		return err
	}

	return nil
}
