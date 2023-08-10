## YAlock: Yet Another (SQL) Lock Library

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

YAlock is a lightweight and easy-to-use SQL-based locking library for Go, designed to provide a simple way to manage and coordinate concurrent access to shared resources.

### Compatibility

⚠️ **Note**:
1. YAlock is tested and compatible with MySQL 8.0+ only. Please ensure you are using a compatible version of MySQL before using this library.
2. This won't work with a MySQL NDB Cluster 

### Installation

```shell
go get github.com/bensooraj/yalock
```

### Usage:
Check out [example_test.go](https://github.com/bensooraj/yalock/blob/main/example_test.go)

https://github.com/bensooraj/yalock/blob/8f0ba7381881c867fc0b1d976fdf7aa82c9f253b/example_test.go#L40-L49

### Why:

One use case is coordinating access to some shared resource or a 3rd party service; we need to guarantee that only one application instance can access it at a time.

For example, coordinating access/updates to a document whose identity (unique ID, authors and any other metadata) and path are stored in MySQL, but the actual object lives in AWS S3 (or any external storage service). 

Another example where I used this in production is that multiple job workers were scheduled to run at specific intervals, each running in its own Docker container. The requirement is that at any given time, only one job can start the processing.

One significant advantage is that no rows or tables are locked, so the main application can access the database for CRUD operations as it should.

