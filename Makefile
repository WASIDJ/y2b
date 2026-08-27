.PHONY: test build deploy clean

COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
VERSION := 1.2.0
LDFLAGS := -s -w -X main.Version=$(VERSION) -X main.BuildCommit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)

test:
	go test -v ./...

build:
	go build -ldflags "$(LDFLAGS)" -o y2b-go main.go

deploy:
	@chmod +x scripts/deploy.sh
	@./scripts/deploy.sh

clean:
	rm -f y2b-go
