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

// Documentation: https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html#function_release-lock
type MySQLLocker struct {
	name string
	db   *sql.DB
}

func (l *MySQLLocker) AcquireLock(ctx context.Context, key string, timeout time.Duration) error {
	var result sql.NullInt16

	row := l.db.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", key, int(timeout.Seconds()))
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
		log.Printf("[AcquireLock::`%s`] failed to acquire lock on `%s` for %d seconds", l.name, key, int(timeout.Seconds()))
	case result.Int16 == 0:
		// for example, because another client has previously locked the name
		log.Printf("[AcquireLock::`%s`] timeout", l.name)
	case result.Int16 == 1:
		// lock was obtained successfully
		log.Printf("[AcquireLock::`%s`] lock acquired on `%s` for %d seconds ", l.name, key, int(timeout.Seconds()))
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
