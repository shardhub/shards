package librarian

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Librarian struct {
	mu        sync.RWMutex
	databases map[string]Database
}

func New() *Librarian {
	return &Librarian{
		mu:        sync.RWMutex{},
		databases: make(map[string]Database),
	}
}

func (l *Librarian) Register(name string, database Database) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if database == nil {
		return errors.New("librarian: Register database is nil")
	}
	if _, ok := l.databases[name]; ok {
		return errors.New("sql: Register called twice for database " + name)
	}

	l.databases[name] = database

	return nil
}

func (l *Librarian) Databases() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	list := make([]string, 0, len(l.databases))
	for name := range l.databases {
		list = append(list, name)
	}

	sort.Strings(list)

	return list
}

func (l *Librarian) Get(name string) Database {
	database, ok := l.databases[name]
	if !ok {
		return nil
	}
	return database
}

type DB struct {
	Database  string
	Username  string
	Password  string
	ExpiredAt *time.Time
}

type CreaterOptions struct {
	Database string
	Username string
	Password *string
	// Set `0` if without TTL
	TTL               time.Duration
	DBNameGenerator   func() string
	UsernameGenerator func() string
	PasswordGenerator func() string
}

type CreaterOption func(*CreaterOptions)

func WithDatabase(database string) CreaterOption {
	return func(o *CreaterOptions) { o.Database = database }
}

func WithUsername(username string) CreaterOption {
	return func(o *CreaterOptions) { o.Username = username }
}

func WithPassword(password string) CreaterOption {
	return func(o *CreaterOptions) { o.Password = &password }
}

func WithTTL(ttl time.Duration) CreaterOption {
	return func(o *CreaterOptions) { o.TTL = ttl }
}

// TODO: Maybe we will add it later.
// func WithoutTTL() CreaterOption {
// 	return func(o *CreaterOptions) { o.TTL = 0 }
// }

type Creater interface {
	CreateDB(ctx context.Context, opts ...CreaterOption) (*DB, error)
}

type Database interface {
	Creater
}

func NewCreaterOptions(opts ...CreaterOption) *CreaterOptions {
	options := &CreaterOptions{
		Database:          "",
		Username:          "",
		Password:          nil,
		TTL:               10 * time.Minute,
		DBNameGenerator:   GenerateDBName,
		UsernameGenerator: GenerateUsername,
		PasswordGenerator: GeneratePassword,
	}

	for _, opt := range opts {
		opt(options)
	}

	return options
}

func GenerateDBName() string {
	// TODO: Make it fancy.
	return "db_" + uuid.New().String()
}

func GenerateUsername() string {
	// TODO: Make it fancy.
	return "user_" + uuid.New().String()
}

func GeneratePassword() string {
	// TODO: Make it fancy.
	return uuid.New().String()
}
