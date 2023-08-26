GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

.PHONY: docker
docker:
	docker build .

.PHONY: license-check
license-check: build
	lichen --config=.lichen.yaml dist/timescaledb-event-streamer

.PHONY: license-header-check
license-header-check:
	license-header-checker -v .license.header.txt . go

.PHONY: build-local
build-local:
	@echo "Building MacOS amd64..."
	GOOS=darwin GOARCH=amd64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/macos/amd64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building MacOS arm64..."
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/macos/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building Linux amd64..."
	GOOS=linux GOARCH=amd64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/amd64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building Linux arm64..."
	GOOS=linux GOARCH=arm64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building Linux Risc V (64bit)..."
	GOOS=linux GOARCH=riscv64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building Windows amd64..."
	GOOS=windows GOARCH=amd64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/windows/amd64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building Windows arm64..."
	GOOS=windows GOARCH=arm64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/windows/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building FreeBSD amd64..."
	GOOS=freebsd GOARCH=amd64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/amd64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building FreeBSD arm64..."
	GOOS=freebsd GOARCH=arm64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer
	@echo "Building FreeBSD Risc V (64bit)..."
	GOOS=freebsd GOARCH=riscv64 go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/linux/arm64/timescaledb-event-streamer ./cmd/timescaledb-event-streamer

.PHONY: build
build:
	go build -v -ldflags "-X 'github.com/noctarius/timescaledb-event-streamer/internal/version.Branch=${GIT_BRANCH}' -X 'github.com/noctarius/timescaledb-event-streamer/internal/version.CommitHash=${GIT_COMMIT}'" -o dist/timescaledb-event-streamer ./cmd/timescaledb-event-streamer

.PHONY: fmt
fmt:
	go fmt ./...
	./.scripts/check_style.sh full

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test: unit-test pg-test

.PHONY: full-test
full-test: unit-test pg-test integration-test

.PHONY: unit-test
unit-test:
	go test -v -race $(shell go list ./... | grep -v 'testsupport' | grep -v 'tests') -timeout 10m

.PHONY: pg-test
pg-test:
	go test -v -race $(shell go list ./... | grep -v 'testsupport' | grep 'tests' | grep -v 'tests/integration') -timeout 40m

.PHONY: integration-test
integration-test: integration-test-aws-kinesis integration-test-aws-sqs integration-test-kafka integration-test-nats integration-test-redis integration-test-redpanda

.PHONY: integration-test-aws-kinesis-test
integration-test-aws-kinesis:
	go test -v -race $(shell go list ./... | grep 'tests/integration/aws_kinesis') -timeout 10m

.PHONY: integration-test-aws-sqs
integration-test-aws-sqs:
	go test -v -race $(shell go list ./... | grep 'tests/integration/aws_sqs') -timeout 10m

.PHONY: integration-test-kafka
integration-test-kafka:
	go test -v -race $(shell go list ./... | grep 'tests/integration/kafka') -timeout 10m

.PHONY: integration-test-nats
integration-test-nats:
	go test -v -race $(shell go list ./... | grep 'tests/integration/nats') -timeout 10m

.PHONY: integration-test-redis
integration-test-redis:
	go test -v -race $(shell go list ./... | grep 'tests/integration/redis') -timeout 10m

.PHONY: integration-test-redpanda
integration-test-redpanda:
	go test -v -race $(shell go list ./... | grep 'tests/integration/redpanda') -timeout 10m

.PHONY: all
all: build test fmt lint
