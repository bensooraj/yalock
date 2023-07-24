package mysql

import (
	"context"
	"database/sql"
	"log"
	"time"
)

func NewMySQLLocker(name string, db *sql.DB) *MySQLLocker {
	return &MySQLLocker{name: name, db: db}
}

type MySQLLocker struct {
	name string
	db   *sql.DB
}

func (l *MySQLLocker) AcquireLock(ctx context.Context, key string, ttl time.Duration) error {
	var result sql.NullString

	row := l.db.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", key, int(ttl.Seconds()))
	if row.Err() != nil {
		return row.Err()
	}
	err := row.Scan(&result)
	if err != nil {
		return err
	}

	switch {
	case !result.Valid: // NULL
		// ... running out of memory or the thread was killed with mysqladmin kill
		log.Printf("[AcquireLock::`%s`] failed to acquire lock on `%s` for %d seconds", l.name, key, int(ttl.Seconds()))
	case result.String == "0":
		log.Printf("[AcquireLock::`%s`] timeout", l.name)
		// for example, because another client has previously locked the name
		// TODO: Check if another client has previously locked the name
	case result.String == "1":
		log.Printf("[AcquireLock::`%s`] lock acquired on `%s` for %d seconds ", l.name, key, int(ttl.Seconds()))
	}

	return nil
}

func (l *MySQLLocker) ReleaseLock(ctx context.Context, key string) error {
	var result sql.NullString
	row := l.db.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", key)
	if row.Err() != nil {
		return row.Err()
	}
	err := row.Scan(&result)
	if err != nil {
		return err
	}
	switch {
	case !result.Valid: // NULL
		log.Printf("[ReleaseLock::`%s`] lock on `%s` doesn't exist", l.name, key)
	case result.String == "0":
		log.Printf("[ReleaseLock::`%s`] lock on `%s` not owned by self", l.name, key)
		// lock was not established by this thread (in which case the lock is not released)
	case result.String == "1":
		log.Printf("[ReleaseLock::`%s`] lock on `%s` released", l.name, key)
	}
	return nil
}

func (l *MySQLLocker) IsLockAcquired(ctx context.Context, key string) (bool, error) {
	var result sql.NullString
	row := l.db.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", key)
	if row.Err() != nil {
		return false, row.Err()
	}
	err := row.Scan(&result)
	if err != nil {
		return false, err
	}
	switch {
	case !result.Valid: // NULL
		log.Printf("[IsLockAcquired::`%s`] lock on `%s` does not exist", l.name, key)
		return false, nil
	default:
		log.Printf("[IsLockAcquired::`%s`] lock on `%s` exists: %s", l.name, key, result.String)
	}
	return false, nil
}
