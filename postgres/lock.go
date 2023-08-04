package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/bensooraj/yalock"
)

func NewPostgreSQLLock(name string, db *sql.DB) *PostgreSQLLock {
	return &PostgreSQLLock{name: name, db: db}
}

// Documentation:
// 1. https://www.postgresql.org/docs/9.1/functions-admin.html
// 2. https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS

type PostgreSQLLock struct {
	name string
	db   *sql.DB
}

func (l *PostgreSQLLock) Name() string {
	return l.name
}

func (l *PostgreSQLLock) AcquireLock(ctx context.Context, key int64, timeout time.Duration) error {
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

	row := l.db.QueryRowContext(ctx, q, key)
	if row.Err() != nil {
		select {
		case <-ctx.Done():
			return &yalock.LockError{
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
			return &yalock.LockError{
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
		return &yalock.LockError{
			Err:         yalock.ErrorLockAcquisitionFailed,
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

func (l *PostgreSQLLock) ReleaseLock(ctx context.Context, key int64) error {
	var result sql.NullBool
	row := l.db.QueryRowContext(ctx, "SELECT pg_advisory_unlock(?)", key)
	if row.Err() != nil {
		return row.Err()
	}
	err := row.Scan(&result)
	if err != nil {
		return err
	}
	switch {
	case !result.Valid: // NULL
	case !result.Bool:
		// lock was not established by this session (in which case the lock is not released)
		return &yalock.LockError{
			Err:         yalock.ErrorLockNotOwned,
			Message:     "lock not owned",
			Method:      "ReleaseLock",
			SessionName: l.name,
			Driver:      "postgres",
		}
	case result.Bool:
		// nothing to do
	}
	return nil
}

func (l *PostgreSQLLock) IsLockAcquired(ctx context.Context, key string) (bool, error) {
	return false, yalock.ErrorNotImplemented
}

func (l *PostgreSQLLock) IsLockFree(ctx context.Context, key string) (bool, error) {
	return false, yalock.ErrorNotImplemented
}

func (l *PostgreSQLLock) ReleaseAllLocks(ctx context.Context) (int, error) {
	row := l.db.QueryRowContext(ctx, "SELECT pg_advisory_unlock_all()")
	if row.Err() != nil {
		return 0, row.Err()
	}
	return 0, nil
}
