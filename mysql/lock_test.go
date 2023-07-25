package mysql_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
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

	// Run the tests
	exitCode := m.Run()

	// Teardown the database
	dbConn1.Close()

	// Exit with the exit code from the tests
	os.Exit(exitCode)
}

func TestLockerBasic(t *testing.T) {
	key := uuid.New().String()
	lockerName := "test-locker-1"

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	locker1 := mysql.NewMySQLLocker(lockerName, dbConn1)

	t.Run("should be able to acquire a lock", func(t *testing.T) {
		err := locker1.AcquireLock(ctx, key, 1*time.Second)
		assert.NoError(t, err, "AcquireLock should not return an error")
	})

	t.Run("the key should be in acquired state", func(t *testing.T) {
		acquired, err := locker1.IsLockAcquired(ctx, key)
		assert.NoError(t, err, "IsLockAcquired should not return an error")
		assert.True(t, acquired, "IsLockAcquired should return true")
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
