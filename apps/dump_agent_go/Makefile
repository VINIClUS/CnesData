.PHONY: lint test build gen install-tools tidy

install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1
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
	go build -o dist/dumpagent ./cmd/dumpagent

build-windows:
	GOOS=windows GOARCH=amd64 go build -trimpath -ldflags="-s -w" \
	  -o dist/dumpagent.exe ./cmd/dumpagent
