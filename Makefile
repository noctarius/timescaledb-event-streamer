GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

.PHONY: docker
docker:
	docker build .

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
