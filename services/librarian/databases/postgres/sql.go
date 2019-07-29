package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/shardhub/shards/pkg/starling"
)

func dropDatabase(ctx context.Context, db *sql.DB, name string) error {
	query := fmt.Sprintf(`DROP DATABASE %s`, pq.QuoteIdentifier(name))

	if _, err := db.ExecContext(ctx, query); err != nil {
		return errors.Wrap(err, "cannot drop database")
	}

	return nil
}

func createDatabase(ctx context.Context, db *sql.DB, name string) error {
	query := fmt.Sprintf(`CREATE DATABASE %s`, pq.QuoteIdentifier(name))

	if _, err := db.ExecContext(ctx, query); err != nil {
		return errors.Wrap(err, "cannot create database")
	}

	return nil
}

func dropUser(ctx context.Context, db starling.ExecContexter, username string) error {
	query := fmt.Sprintf(`DROP USER IF EXISTS %s`, pq.QuoteIdentifier(username))

	if _, err := db.ExecContext(ctx, query); err != nil {
		return errors.Wrap(err, "cannot drop user")
	}

	return nil
}

func createUser(ctx context.Context, db starling.ExecContexter, username, password string) error {
	query := fmt.Sprintf(`CREATE USER %s WITH ENCRYPTED PASSWORD %s`, pq.QuoteIdentifier(username), pq.QuoteLiteral(password))

	if _, err := db.ExecContext(ctx, query); err != nil {
		return errors.Wrap(err, "cannot create user")
	}

	return nil
}

func grantAllPrivileges(ctx context.Context, db starling.ExecContexter, database, username string) error {
	query := fmt.Sprintf(`GRANT ALL PRIVILEGES ON DATABASE %s TO %s`, pq.QuoteIdentifier(database), pq.QuoteIdentifier(username))

	if _, err := db.ExecContext(ctx, query); err != nil {
		return errors.Wrap(err, "cannot grand privileges to user")
	}

	return nil
}
