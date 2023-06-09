GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

.PHONY: docker
docker:
	docker build .

.PHONY: build-local
build-local:
	@echo Building MacOS amd64...
	GOOS=darwin GOARCH=amd64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/macos/amd64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo Building MacOS arm64...
	GOOS=darwin GOARCH=arm64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/macos/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo Building Linux amd64...
	GOOS=linux GOARCH=amd64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/amd64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building Linux arm (32 bit)..."
	GOOS=linux GOARCH=arm go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/arm32/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo Building Linux arm64...
	GOOS=linux GOARCH=arm64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo Building Windows amd64...
	GOOS=windows GOARCH=amd64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/windows/amd64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo Building Windows arm64...
	GOOS=windows GOARCH=arm64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/windows/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer

.PHONY: build
build:
	go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/timescaledb-event-streamer ./cmd/timescaledb-event-streamer

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test: unit-test integration-test

.PHONY: unit-test
unit-test:
	go test -v -race $(shell go list ./... | grep -v 'testing' | grep -v 'tests/integration') -timeout 10m

.PHONY: integration-test
integration-test:
	go test -v -race $(shell go list ./... | grep 'tests/integration') -timeout 40m

.PHONY: all
all: build test fmt lint
