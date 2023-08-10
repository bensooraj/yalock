package yalock_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	mysqlYAlock "github.com/bensooraj/yalock/mysql"
)

func Example() {
	// Flags
	var (
		dbUsername = flag.String("db-username", "", "database username")
		dbPassword = flag.String("db-password", "", "database password")
		dbHost     = flag.String("db-host", "", "database host")
		dbPort     = flag.String("db-port", "", "database port")
		dbName     = flag.String("db-name", "", "database name")
	)

	// Create the database connection
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", *dbUsername, *dbPassword, *dbHost, *dbPort, *dbName))
	if err != nil {
		log.Fatal("error establishing a db connection", err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal("error pinging the db", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

	// Create a lock
	lockKeyName := "name-of-the-key-on-which-lock-is-acquired"
	lockName := "unique-name-of-the-lock"

	lock := mysqlYAlock.NewMySQLLock(lockName, db)
	err = lock.AcquireLock(ctx, lockKeyName, 1*time.Second)
	if err != nil {
		log.Fatal("error acquiring lock", err)
	}
	defer lock.ReleaseLock(ctx, lockKeyName)

	// proceed with the processing
	// ...

}
