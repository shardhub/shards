package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/shardhub/shards/pkg/starling"
	"github.com/shardhub/shards/services/librarian"
)

// nowFunc returns the current time; it's overridden in tests.
var nowFunc = time.Now // nolint:gochecknoglobals

var _ librarian.Database = (*Postgres)(nil)

type Option func(*Postgres)

func WithScheme(scheme string) Option {
	return func(o *Postgres) { o.scheme = scheme }
}

func WithHost(host string) Option {
	return func(o *Postgres) { o.host = host }
}

func WithPort(port int) Option {
	return func(o *Postgres) { o.port = port }
}

func WithUsername(username string) Option {
	return func(o *Postgres) { o.username = username }
}

func WithPassword(password string) Option {
	return func(o *Postgres) { o.password = password }
}

func WithManagementDatabase(database string) Option {
	return func(o *Postgres) { o.managementDatabase = database }
}

func WithSoftDelete() Option {
	return func(o *Postgres) { o.softDelete = true }
}

type Postgres struct {
	scheme             string
	host               string
	port               int
	username           string
	password           string
	managementDatabase string
	softDelete         bool

	rootDB       *sql.DB
	managementDB *sql.DB
}

type database struct {
	ID        int
	Name      string
	ExpiredAt *time.Time
	Users     []user
}

type user struct {
	ID       int
	Username string
}

func New(opts ...Option) *Postgres {
	p := &Postgres{
		scheme:             "postgres",
		host:               "localhost",
		port:               5432,
		username:           "postgres",
		password:           "",
		managementDatabase: "librarian",
		softDelete:         false,

		rootDB:       nil,
		managementDB: nil,
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *Postgres) Connect(ctx context.Context) error {
	db, err := connect(ctx, &connectOptions{
		Scheme:   p.scheme,
		Host:     p.host,
		Port:     p.port,
		Database: "",
		Username: p.username,
		Password: p.password,
	})
	if err != nil {
		return errors.Wrap(err, "cannot connect to root database")
	}

	p.rootDB = db

	return nil
}

func (p *Postgres) Disconnect() error {
	if p.rootDB != nil {
		if err := p.rootDB.Close(); err != nil {
			return errors.Wrap(err, "cannot close connection with root DB")
		}
	}

	if p.managementDB != nil {
		if err := p.managementDB.Close(); err != nil {
			return errors.Wrap(err, "cannot close connection with management DB")
		}
	}

	return nil
}

func (p *Postgres) Init(ctx context.Context) error {
	if err := p.createManagementDB(ctx); err != nil {
		const duplicateDatabaseCode = "42P04"

		if e, ok := errors.Cause(err).(*pq.Error); ok && e.Code == duplicateDatabaseCode {
			// That's ok
		} else {
			return errors.Wrap(err, "cannot create management database")
		}
	}

	if err := p.connectManagementDB(ctx); err != nil {
		return errors.Wrap(err, "cannot connect to management DB")
	}

	if err := p.createManagementTables(ctx); err != nil {
		const duplicateTableCode = "42P07"

		if e, ok := errors.Cause(err).(*pq.Error); ok && e.Code == duplicateTableCode {
			// That's ok
		} else {
			return errors.Wrap(err, "cannot create management tables")
		}
	}

	// TODO: Add migrations

	return nil
}

func (p *Postgres) Create(ctx context.Context, opts ...librarian.CreaterOption) (*librarian.DB, error) {
	options := librarian.NewCreaterOptions(opts...)

	now := nowFunc()

	database := options.Database
	if database == "" {
		database = options.DBNameGenerator()
	}

	username := options.Username
	if username == "" {
		username = options.UsernameGenerator()
	}

	password := ""
	if options.Password != nil {
		password = *options.Password
	} else {
		password = options.PasswordGenerator()
	}

	var (
		dbID int
		err  error
	)

	// Database
	err = starling.Transaction(ctx, p.managementDB, func(tx *sql.Tx) error {
		// Insert database
		id, err := p.insertDatabase(ctx, tx, database, now, options.TTL)
		if err != nil {
			return errors.Wrap(err, "cannot insert database")
		}

		// NOTE: we do it in transaction because we want to rollback insertions
		// if we won't create a DB.

		// Create database
		if err := createDatabase(ctx, p.rootDB, database); err != nil {
			return errors.Wrap(err, "cannot create database")
		}

		dbID = id

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot create DB")
	}

	// User
	err = starling.Transaction(ctx, p.managementDB, func(tx *sql.Tx) error {
		// Insert user
		_, err = p.insertUser(ctx, tx, dbID, username, now)
		if err != nil {
			return errors.Wrap(err, "cannot insert user")
		}

		// NOTE: we do it in transaction because we want to rollback insertions
		// if we won't create a user.

		// Create user
		if err := createUser(ctx, p.rootDB, username, password); err != nil {
			return errors.Wrap(err, "cannot create user")
		}

		// Create user
		if err := grantAllPrivileges(ctx, p.rootDB, database, username); err != nil {
			return errors.Wrap(err, "cannot grand all privileges to user")
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot create user")
	}

	var expiredAt *time.Time
	if options.TTL != 0 {
		v := now.Add(options.TTL)

		expiredAt = &v
	}

	return &librarian.DB{
		Database:  database,
		Username:  username,
		Password:  password,
		ExpiredAt: expiredAt,
	}, nil
}

func (p *Postgres) List(ctx context.Context) ([]librarian.DB, error) {
	now := nowFunc()

	databases, err := p.list(ctx, p.managementDB, now, true)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get list of DBs")
	}

	dbs := make([]librarian.DB, 0, len(databases))
	for _, db := range databases {
		for _, user := range db.Users {
			dbs = append(dbs, librarian.DB{
				Database:  db.Name,
				Username:  user.Username,
				Password:  "",
				ExpiredAt: db.ExpiredAt,
			})
		}
	}

	return dbs, nil
}

func (p *Postgres) DeleteExpired(ctx context.Context) ([]librarian.DB, error) {
	now := nowFunc()

	var deletedDBs []librarian.DB

	err := starling.Transaction(ctx, p.managementDB, func(tx *sql.Tx) error {
		databases, err := p.list(ctx, p.managementDB, now, true)
		if err != nil {
			return errors.Wrap(err, "cannot get list of expired DBs")
		}

		// Drop databases
		for _, database := range databases {
			if err := dropDatabase(ctx, p.rootDB, database.Name); err != nil {
				return errors.Wrap(err, "cannot drop database")
			}
		}

		// Drop users
		for _, database := range databases {
			for _, user := range database.Users {
				if err := dropUser(ctx, p.rootDB, user.Username); err != nil {
					return errors.Wrap(err, "cannot drop user")
				}
			}
		}

		// Delete users
		for _, database := range databases {
			for _, user := range database.Users {
				if p.softDelete {
					_, err = tx.ExecContext(ctx, `
						UPDATE users
						SET deleted_at = $1
						WHERE id = $2
					`, now, user.ID)
					if err != nil {
						return errors.Wrap(err, "cannot delete user from users")
					}
				} else {
					_, err = tx.ExecContext(ctx, `
						DELETE FROM users
						WHERE id = $1
					`, user.ID)
					if err != nil {
						return errors.Wrap(err, "cannot delete user from users")
					}
				}
			}
		}

		// Delete databases
		for _, database := range databases {
			if p.softDelete {
				_, err = tx.ExecContext(ctx, `
					UPDATE databases
					SET deleted_at = $1
					WHERE id = $2
				`, now, database.ID)
				if err != nil {
					return errors.Wrap(err, "cannot delete user from users")
				}
			} else {
				_, err = tx.ExecContext(ctx, `
					DELETE FROM databases
					WHERE id = $1
				`, database.ID)
				if err != nil {
					return errors.Wrap(err, "cannot delete database from databases")
				}
			}
		}

		deletedDBs = make([]librarian.DB, 0, len(databases))
		for _, db := range databases {
			for _, user := range db.Users {
				deletedDBs = append(deletedDBs, librarian.DB{
					Database:  db.Name,
					Username:  user.Username,
					Password:  "",
					ExpiredAt: db.ExpiredAt,
				})
			}
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot delete expired DBs")
	}

	return deletedDBs, nil
}

func (p *Postgres) createManagementDB(ctx context.Context) error {
	if err := createDatabase(ctx, p.rootDB, p.managementDatabase); err != nil {
		return errors.Wrap(err, "cannot create management database")
	}

	return nil
}

func (p *Postgres) connectManagementDB(ctx context.Context) error {
	db, err := connect(ctx, &connectOptions{
		Scheme:   p.scheme,
		Host:     p.host,
		Port:     p.port,
		Database: p.managementDatabase,
		Username: p.username,
		Password: p.password,
	})
	if err != nil {
		return errors.Wrap(err, "cannot connect to management database")
	}

	p.managementDB = db

	return nil
}

func (p *Postgres) createManagementTables(ctx context.Context) error {
	return starling.Transaction(ctx, p.managementDB, func(tx *sql.Tx) error {
		var err error

		// Create databases table
		_, err = tx.ExecContext(ctx, `
			CREATE TABLE databases (
				id SERIAL,
				name VARCHAR(255) NOT NULL,
				expired_at TIMESTAMP WITH TIME ZONE,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				deleted_at TIMESTAMP WITH TIME ZONE,

				CONSTRAINT pk__databases__id PRIMARY KEY (id),
				CONSTRAINT ux__databases__name UNIQUE (name)
			)
		`)
		if err != nil {
			return errors.Wrap(err, `cannot create "databases" table`)
		}

		// Create users table
		_, err = tx.ExecContext(ctx, `
			CREATE TABLE users (
				id SERIAL,
				username VARCHAR(255) NOT NULL,
				database_id INT NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				deleted_at TIMESTAMP WITH TIME ZONE,

				CONSTRAINT pk__users__id PRIMARY KEY (id),
				CONSTRAINT ux__users__name UNIQUE (username),
				CONSTRAINT fk__users__database_id FOREIGN KEY (database_id) REFERENCES databases(id)
			)
		`)
		if err != nil {
			return errors.Wrap(err, `cannot create "users" table`)
		}

		return nil
	})
}

func (p *Postgres) insertDatabase(ctx context.Context, db starling.QueryRowContexter, database string, now time.Time, ttl time.Duration) (int, error) {
	row := db.QueryRowContext(ctx, `
		INSERT INTO databases (name, created_at, expired_at)
		VALUES ($1, $2, $3)
		RETURNING id
	`, database, now, now.Add(ttl))

	var id int
	if err := row.Scan(&id); err != nil {
		return 0, errors.Wrap(err, "cannot scan database id")
	}

	return id, nil
}

func (p *Postgres) insertUser(ctx context.Context, db starling.QueryRowContexter, databaseID int, username string, now time.Time) (int, error) {
	row := db.QueryRowContext(ctx, `
		INSERT INTO users (username, database_id, created_at)
		VALUES ($1, $2, $3)
		RETURNING id
	`, username, databaseID, now)

	var id int
	if err := row.Scan(&id); err != nil {
		return 0, errors.Wrap(err, "cannot scan user id")
	}

	return id, nil
}

func (p *Postgres) list(ctx context.Context, db starling.QueryContexter, now time.Time, notExired bool) ([]database, error) {
	var rows *sql.Rows
	var err error

	if notExired {
		rows, err = db.QueryContext(ctx, `
			SELECT d.id, d.name, d.expired_at, u.id, u.username
			FROM databases AS d
			LEFT JOIN users AS u
			ON u.database_id = d.id
			WHERE d.deleted_at IS NULL AND d.expired_at IS NOT NULL AND d.expired_at < $1
		`, now)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT d.id, d.name, d.expired_at, u.id, u.username
			FROM databases AS d
			LEFT JOIN users AS u
			ON u.database_id = d.id
			WHERE d.expired_at < $1
		`)
	}
	if err != nil {
		return nil, errors.Wrap(err, "cannot select expired databases")
	}
	defer rows.Close() // nolint:gosec,errcheck

	mappedDBs := make(map[int]database)
	for rows.Next() {
		var dtbs struct {
			ID        int
			Name      string
			ExpiredAt *time.Time
			User      struct {
				ID       int
				Username string
			}
		}

		if err := rows.Scan(&dtbs.ID, &dtbs.Name, &dtbs.ExpiredAt, &dtbs.User.ID, &dtbs.User.Username); err != nil {
			return nil, errors.Wrap(err, "cannot scan database")
		}

		d, ok := mappedDBs[dtbs.ID]
		if !ok {
			d = database{
				ID:        dtbs.ID,
				Name:      dtbs.Name,
				Users:     nil,
				ExpiredAt: dtbs.ExpiredAt,
			}
		}

		d.Users = append(d.Users, user{
			ID:       dtbs.User.ID,
			Username: dtbs.User.Username,
		})
		mappedDBs[dtbs.ID] = d
	}

	databases := make([]database, 0, len(mappedDBs))
	for _, db := range mappedDBs {
		databases = append(databases, db)
	}

	return databases, nil
}
