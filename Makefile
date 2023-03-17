GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/timescaledb-event-streamer ./cmd/timescaledb-event-streamer

.PHONY: go-fmt
go-fmt:
	go fmt ./...

.PHONY: go-lint
go-lint:
	golangci-lint run

.PHONY: test
test:
	go test -v -race $(shell go list ./... |grep -v '_test.go') -timeout 40m

.PHONY: all
all: build test go-fmt go-lint
