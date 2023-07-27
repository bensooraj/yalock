package mysql_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bensooraj/yalock/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"log"
)

var (
	driverName = flag.String("driver-name", "", "database driver name")
	dbUsername = flag.String("db-username", "", "database username")
	dbPassword = flag.String("db-password", "", "database password")
	dbHost     = flag.String("db-host", "", "database host")
	dbPort     = flag.String("db-port", "", "database port")
	dbName     = flag.String("db-name", "", "database name")
)

var (
	dbConn1 *sql.DB
	dbConn2 *sql.DB
)

func TestMain(m *testing.M) {
	// Parse the flags
	flag.Parse()

	// If the driver name is not mysql, skip the tests
	if !isFlagPassed("driver-name") || *driverName != "mysql" {
		log.Println("Skipping tests for driver:", *driverName)
		os.Exit(0)
	}
	verifyFlags()

	// Setup the database
	dbConn1 = CreateDBConnection()
	dbConn2 = CreateDBConnection()

	// Run the tests
	exitCode := m.Run()

	// Teardown the database
	dbConn1.Close()
	dbConn2.Close()

	// Exit with the exit code from the tests
	os.Exit(exitCode)
}

func TestLock_Basic(t *testing.T) {
	key := uuid.New().String()
	lockName := "test-lock-1"

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	lock1 := mysql.NewMySQLLock(lockName, dbConn1)

	t.Run("should be able to acquire a lock", func(t *testing.T) {
		err := lock1.AcquireLock(ctx, key, 1*time.Second)
		assert.NoError(t, err, "AcquireLock should not return an error")
	})

	t.Run("the key should be in acquired state", func(t *testing.T) {
		acquired, err := lock1.IsLockAcquired(ctx, key)
		assert.NoError(t, err, "IsLockAcquired should not return an error")
		assert.True(t, acquired, "IsLockAcquired should return true")
	})

	t.Run("the key should not be free", func(t *testing.T) {
		free, err := lock1.IsLockFree(ctx, key)
		assert.NoError(t, err, "IsLockFree should not return an error")
		assert.False(t, free, "IsLockFree should return false")
	})

	t.Run("should be able to release the lock", func(t *testing.T) {
		err := lock1.ReleaseLock(ctx, key)
		assert.NoError(t, err, "ReleaseLock should not return an error")
	})

	t.Run("the key should be in free state after release", func(t *testing.T) {
		free, err := lock1.IsLockFree(ctx, key)
		assert.NoError(t, err, "IsLockFree should not return an error")
		assert.True(t, free, "IsLockFree should return true")
	})

	t.Run("the key should not be in acquired state after release", func(t *testing.T) {
		acquired, err := lock1.IsLockAcquired(ctx, key)
		assert.NoError(t, err, "IsLockAcquired should not return an error")
		assert.False(t, acquired, "IsLockAcquired should return false")
	})

	t.Run("should have zero keys to release", func(t *testing.T) {
		n, err := lock1.ReleaseAllLocks(ctx)
		assert.NoError(t, err, "ReleaseAllLocks should not return an error")
		assert.Equal(t, 0, n, "ReleaseAllLocks should return 0")
	})
}

func TestLock_TwoLocksSequential(t *testing.T) {
	key := uuid.New().String()

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	lockName := "test-lock-1"
	lock1 := mysql.NewMySQLLock(lockName, dbConn1)

	lockName = "test-lock-2"
	lock2 := mysql.NewMySQLLock(lockName, dbConn2)

	t.Run(fmt.Sprintf("%s should be able to acquire a lock", lock1.Name()), func(t *testing.T) {
		err := lock1.AcquireLock(ctx, key, 1*time.Second)
		assert.NoError(t, err, "AcquireLock should not return an error")
	})

	t.Run(fmt.Sprintf("%s should not be able to acquire the same lock", lock2.Name()), func(t *testing.T) {
		err := lock2.AcquireLock(ctx, key, 1*time.Second)
		assert.Error(t, err, "AcquireLock should return an error")
	})

	t.Run(fmt.Sprintf("%s should be able to release the lock acquired by %s", lock2.Name(), lock1.Name()), func(t *testing.T) {
		err := lock2.ReleaseLock(ctx, key)
		assert.ErrorIs(t, err, mysql.ErrorLockNotOwned, "ReleaseLock should return ErrorLockNotOwned")
	})

	t.Run(fmt.Sprintf("%s should be able to release the lock", lock1.Name()), func(t *testing.T) {
		err := lock1.ReleaseLock(ctx, key)
		assert.NoError(t, err, "ReleaseLock should not return an error")
	})

	t.Run(fmt.Sprintf("%s should be able to acquire the same lock", lock2.Name()), func(t *testing.T) {
		err := lock2.AcquireLock(ctx, key, 1*time.Second)
		assert.NoError(t, err, "AcquireLock should not return an error")
	})

	t.Run(fmt.Sprintf("%s should be able to release the lock", lock2.Name()), func(t *testing.T) {
		err := lock2.ReleaseLock(ctx, key)
		assert.NoError(t, err, "ReleaseLock should not return an error")
	})
}

func TestLock_TwoLockParallel(t *testing.T) {
	key := uuid.New().String()

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	lockName := "test-lock-1"
	lock1 := mysql.NewMySQLLock(lockName, dbConn1)

	lockName = "test-lock-2"
	lock2 := mysql.NewMySQLLock(lockName, dbConn2)

	t.Run(fmt.Sprintf("%s should be able to acquire a lock", lock1.Name()), func(t *testing.T) {
		var num atomic.Int32

		wg := sync.WaitGroup{}

		for _, lock := range []*mysql.MySQLLock{lock1, lock2} {
			wg.Add(1)

			go func(lock *mysql.MySQLLock) {
				defer wg.Done()

				err := lock.AcquireLock(ctx, key, 1*time.Second)
				if err == nil {
					time.Sleep(3 * time.Second) // simulate processing time
					num.Add(1)                  // increment the value
				}
				defer lock.ReleaseLock(ctx, key)

			}(lock)
		}
		wg.Wait()
		assert.Equal(t, int32(1), num.Load(), "only one locker should be able to acquire the lock to update the value")
	})

	t.Run(fmt.Sprintf("%s should have no locks to release", lock1.Name()), func(t *testing.T) {
		n, err := lock1.ReleaseAllLocks(ctx)
		assert.NoError(t, err, "ReleaseAllLocks should not return an error")
		assert.Equal(t, 0, n, "ReleaseAllLocks should return 0")
	})

	t.Run(fmt.Sprintf("%s should have no locks to release", lock2.Name()), func(t *testing.T) {
		n, err := lock2.ReleaseAllLocks(ctx)
		assert.NoError(t, err, "ReleaseAllLocks should not return an error")
		assert.Equal(t, 0, n, "ReleaseAllLocks should return 0")
	})
}

func TestLock_Timeouts(t *testing.T) {
	key := uuid.New().String()

	lockName := "test-lock-1"
	lock1 := mysql.NewMySQLLock(lockName, dbConn1)

	lockName = "test-lock-2"
	lock2 := mysql.NewMySQLLock(lockName, dbConn2)

	t.Run(fmt.Sprintf("%s should timeout upon context timeout", lock2.Name()), func(t *testing.T) {
		// Contect valid for 2 seconds
		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

		// Locker 1 acquires the lock with a timeout of 0 second (almost immediately)
		err := lock1.AcquireLock(ctx, key, 0*time.Second)
		assert.NoError(t, err, "AcquireLock should not return an error")
		defer lock1.ReleaseLock(ctx, key)

		// Locker 2 attempts to acquire the lock with a timeout of -1 (blocking, inifiite waiting time)
		err = lock2.AcquireLock(ctx, key, -1*time.Second)
		assert.ErrorIs(t, err, context.DeadlineExceeded, "AcquireLock should return context.DeadlineExceeded")
	})

	t.Run(fmt.Sprintf("%s should timeout upon context cancellation", lock2.Name()), func(t *testing.T) {
		// Contect with cancel
		ctx, cancel := context.WithCancel(context.Background())

		// Locker 1 acquires the lock with a timeout of 0 second (almost immediately)
		err := lock1.AcquireLock(ctx, key, 0*time.Second)
		assert.NoError(t, err, "AcquireLock should not return an error")
		defer lock1.ReleaseLock(ctx, key)

		// Locker 2 attempts to acquire the lock with a timeout of -1 (blocking, inifiite waiting time)
		time.AfterFunc(1*time.Second, cancel)
		err = lock2.AcquireLock(ctx, key, -1*time.Second)
		assert.ErrorIs(t, err, context.Canceled, "AcquireLock should return context.Canceled")
	})

	t.Run(fmt.Sprintf("%s should timeout waiting on %s", lock2.Name(), lock1.Name()), func(t *testing.T) {
		ctx := context.Background()

		// Locker 1 acquires the lock with a timeout of 0 second (almost immediately)
		err := lock1.AcquireLock(ctx, key, 0*time.Second)
		assert.NoError(t, err, "AcquireLock should not return an error")
		defer lock1.ReleaseLock(ctx, key)

		// Locker 2 attempts to acquire the lock with a timeout of 1 second
		err = lock2.AcquireLock(ctx, key, 1*time.Second)
		assert.ErrorIs(t, err, mysql.ErrorLockTimeout, "AcquireLock should return ErrorLockTimeout")
	})
}

func CreateDBConnection() *sql.DB {
	// Create the database connection
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", *dbUsername, *dbPassword, *dbHost, *dbPort, *dbName))
	if err != nil {
		log.Fatal("CreateDBConnection::Open", err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal("CreateDBConnection::Ping", err)
	}
	return db
}

func verifyFlags() {
	if !isFlagPassed("driver-name") || *driverName == "" {
		log.Fatal("driver-name is required")
	}
	if !isFlagPassed("db-username") || *dbUsername == "" {
		log.Fatal("db-username is required")
	}
	if !isFlagPassed("db-password") || *dbPassword == "" {
		log.Fatal("db-password is required")
	}
	if !isFlagPassed("db-host") || *dbHost == "" {
		log.Fatal("db-host is required")
	}
	if !isFlagPassed("db-port") || *dbPort == "" {
		log.Fatal("db-port is required")
	}
	if !isFlagPassed("db-name") || *dbName == "" {
		log.Fatal("db-name is required")
	}
}

func isFlagPassed(name string) bool {

	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
