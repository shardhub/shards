package postgres

import (
	"database/sql"
	"net/url"
	"strconv"

	"github.com/pkg/errors"
)

type connectOptions struct {
	Scheme   string
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

func connect(options *connectOptions) (*sql.DB, error) {
	q := url.Values{
		"sslmode": []string{"disable"}, // TODO
	}

	dsn := &url.URL{
		Scheme:   options.Scheme,
		User:     url.UserPassword(options.Username, options.Password),
		Host:     options.Host + ":" + strconv.Itoa(options.Port),
		Path:     "/" + options.Database,
		RawQuery: q.Encode(),
	}

	db, err := sql.Open(dsn.Scheme, dsn.String())
	if err != nil {
		return nil, errors.Wrap(err, "cannot open connection")
	}

	if err := db.Ping(); err != nil {
		if cerr := db.Close(); cerr != nil {
			return nil, errors.Wrap(err, "cannot close connection")
		}

		return nil, errors.Wrap(err, "cannot ping database")
	}

	return db, nil
}
