package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func NewPostgreSQLLock(name string, db *sql.DB) *PostgreSQLLock {
	return &PostgreSQLLock{name: name, db: db}
}

// Documentation: https://www.postgresql.org/docs/9.1/functions-admin.html
type PostgreSQLLock struct {
	name string
	db   *sql.DB
}

func (l *PostgreSQLLock) Name() string {
	return l.name
}

func (l *PostgreSQLLock) AcquireLock(ctx context.Context, key interface{}, timeout time.Duration) error {
	// validate the arguments passments
	keyI, ok := key.(int64)
	if !ok {
		return &LockError{
			Err:         errors.New("key must be a 64-bit integer"),
			Message:     "key must be a 64-bit integer",
			Method:      "AcquireLock",
			SessionName: l.name,
			Driver:      "postgres",
		}
	}

	var (
		result sql.NullBool
		q string
	)
	// if timeout is negative, then wait indefinitely
	if timeout < 0 {
		q = "SELECT pg_advisory_lock(?)"
	} else {	
		q = "SELECT pg_try_advisory_lock(?)"
	}

	row := l.db.QueryRowContext(ctx, q, keyI)
	if row.Err() != nil {
		select {
		case <-ctx.Done():
			return &LockError{
				Err:         ctx.Err(),
				Message:     "context deadline exceeded while querying row",
				Method:      "AcquireLock",
				SessionName: l.name,
				Driver:      "postgres",
			}
		default:
			return row.Err()
		}
	}
	err := row.Scan(&result)
	if err != nil {
		select {
		case <-ctx.Done():
			return &LockError{
				Err:         ctx.Err(),
				Message:     "context deadline exceeded while scanning row",
				Method:      "AcquireLock",
				SessionName: l.name,
				Driver:      "postgres",
			}
		default:
			return err
		}
	}

	switch {
	case !result.Valid: // NULL
	case !result.Bool:
		return &LockError{
			Err:         ErrorLockAcquisitionFailed,
			Message:     "failed to acquire lock",
			Method:      "AcquireLock",
			SessionName: l.name,
			Driver:      "postgres",
		}
	case result.Bool:
		// lock was obtained successfully
	}
	return nil
}

func (l *PostgreSQLLock) ReleaseLock(ctx context.Context, key string) error {
	var result sql.NullInt16
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
		// the named lock did not exist
		return &LockError{
			Err:         ErrorLockDoesNotExist,
			Message:     "lock does not exist",
			Method:      "ReleaseLock",
			SessionName: l.name,
			Driver:      "postgres",
		}
	case result.Int16 == 0:
		// lock was not established by this thread (in which case the lock is not released)
		return &LockError{
			Err:         ErrorLockNotOwned,
			Message:     "lock not owned",
			Method:      "ReleaseLock",
			SessionName: l.name,
			Driver:      "postgres",
		}
	case result.Int16 == 1:
		// log.Printf("[ReleaseLock::`%s`] lock on `%s` released", l.name, key)
	}
	return nil
}

func (l *PostgreSQLLock) IsLockAcquired(ctx context.Context, key string) (bool, error) {
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
		return false, nil
	default:
		return true, nil
	}
}

func (l *PostgreSQLLock) IsLockFree(ctx context.Context, key string) (bool, error) {
	var result sql.NullInt16
	row := l.db.QueryRowContext(ctx, "SELECT IS_FREE_LOCK(?)", key)
	if row.Err() != nil {
		return false, row.Err()
	}
	err := row.Scan(&result)
	if err != nil {
		return false, err
	}

	switch {
	case !result.Valid: // NULL
		// if an error occurs (such as an incorrect argument)
		return false, &LockError{
			Err:         ErrorLockUnknown,
			Message:     "unknown error (possibly an incorrect argument)",
			Method:      "IsLockFree",
			SessionName: l.name,
			Driver:      "postgres",
		}
	case result.Int16 == 0:
		// Lock is in use
		return false, nil
	case result.Int16 == 1:
		// Lock is free (no one is using the lock)
		return true, nil
	}
	return false, nil
}

func (l *PostgreSQLLock) ReleaseAllLocks(ctx context.Context) (int, error) {
	var result sql.NullInt32
	row := l.db.QueryRowContext(ctx, "SELECT RELEASE_ALL_LOCKS()")
	if row.Err() != nil {
		return 0, row.Err()
	}
	err := row.Scan(&result)
	if err != nil {
		return 0, err
	}
	return int(result.Int32), nil
}
