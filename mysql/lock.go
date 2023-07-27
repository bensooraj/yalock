package mysql

import (
	"context"
	"database/sql"
	"time"
)

func NewMySQLLock(name string, db *sql.DB) *MySQLLock {
	return &MySQLLock{name: name, db: db}
}

// Documentation: https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html#function_release-lock
type MySQLLock struct {
	name string
	db   *sql.DB
}

func (l *MySQLLock) Name() string {
	return l.name
}

func (l *MySQLLock) AcquireLock(ctx context.Context, key string, timeout time.Duration) error {
	var result sql.NullInt16

	row := l.db.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", key, int(timeout.Seconds()))
	if row.Err() != nil {
		select {
		case <-ctx.Done():
			return &LockError{
				Err:         ctx.Err(),
				Message:     "context deadline exceeded while querying row",
				Method:      "AcquireLock",
				SessionName: l.name,
				Driver:      "mysql",
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
				Driver:      "mysql",
			}
		default:
			return err
		}
	}

	switch {
	case !result.Valid: // NULL
		// ... running out of memory or the thread was killed with mysqladmin kill
		return &LockError{
			Err:         ErrorLockAcquisitionFailed,
			Message:     "failed to acquire lock",
			Method:      "AcquireLock",
			SessionName: l.name,
			Driver:      "mysql",
		}
	case result.Int16 == 0:
		// for example, because another client has previously locked the name
		return &LockError{
			Err:         ErrorLockTimeout,
			Message:     "timeout",
			Method:      "AcquireLock",
			SessionName: l.name,
			Driver:      "mysql",
		}
	case result.Int16 == 1:
		// lock was obtained successfully
		// log.Printf("[AcquireLock::`%s`] lock acquired on `%s` with a timeout of %d seconds ", l.name, key, int(timeout.Seconds()))
	}
	return nil
}

func (l *MySQLLock) ReleaseLock(ctx context.Context, key string) error {
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
			Driver:      "mysql",
		}
	case result.Int16 == 0:
		// lock was not established by this thread (in which case the lock is not released)
		return &LockError{
			Err:         ErrorLockNotOwned,
			Message:     "lock not owned",
			Method:      "ReleaseLock",
			SessionName: l.name,
			Driver:      "mysql",
		}
	case result.Int16 == 1:
		// log.Printf("[ReleaseLock::`%s`] lock on `%s` released", l.name, key)
	}
	return nil
}

func (l *MySQLLock) IsLockAcquired(ctx context.Context, key string) (bool, error) {
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

func (l *MySQLLock) IsLockFree(ctx context.Context, key string) (bool, error) {
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
			Driver:      "mysql",
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

func (l *MySQLLock) ReleaseAllLocks(ctx context.Context) (int, error) {
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
