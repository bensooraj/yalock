include .env

test-mysql:
	@go test -race -v -cover -coverprofile=coverage.out ./mysql \
		-driver-name=mysql \
		-db-username=$(DB_USERNAME) \
		-db-password=$(DB_PASSWORD) \
		-db-host=$(DB_HOST) \
		-db-port=$(DB_PORT) \
		-db-name=$(DB_NAME)

fmt:
	@go fmt ./...

tidy:
	@go mod tidy