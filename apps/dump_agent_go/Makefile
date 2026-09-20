.PHONY: lint test build gen install-tools tidy build-linux build-windows

GIT_VERSION := $(shell git describe --tags --match 'dumpagent-go-v*' --dirty 2>/dev/null \
                 | sed 's/^dumpagent-go-//')
ifeq ($(strip $(GIT_VERSION)),)
GIT_VERSION := dev
endif
VERSION ?= $(GIT_VERSION)

install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.0
	go install go.uber.org/mock/mockgen@v0.5.0

gen:
	go generate ./...

lint:
	golangci-lint run --timeout=5m
	go vet ./...

tidy:
	go mod tidy

test:
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

test-e2e:
	go test -tags=e2e -race ./test/e2e/...

build-linux:
	go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION)" \
	  -o dist/dumpagent ./cmd/dumpagent

build-windows:
	GOOS=windows GOARCH=amd64 go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION)" \
	  -o dist/dumpagent.exe ./cmd/dumpagent
