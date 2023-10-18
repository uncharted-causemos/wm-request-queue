export GOPROXY=direct

VERSION=`git describe --tags`
TIMESTAMP=`date +%FT%T%z`

LDFLAGS=-ldflags "-X main.version=${VERSION} -X main.timestamp=${TIMESTAMP}"

.PHONY: all

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  run           - run the queue server"
	@echo "  build         - build the source code"
	@echo "  fmt           - format the source code"
	@echo "  lint          - lint the source code"
	@echo "  install       - install dev dependencies"
	@echo "  test          - run tests"

run:
	@go run ./main.go

lint:
	@go vet ./...
	@go list ./... | grep -v /vendor/ | xargs -L1 golint --set_exit_status

fmt:
	@go fmt ./...

build: lint
	@go build ${LDFLAGS}

build_static:
	@env CGO_ENABLED=0 env GOOS=linux GOARCH=amd64 go build ${LDFLAGS}

compile: lint
	@go build ./...

test: build
	@go test -race -cover $$(go list ./...)

install:
	@go install gitlab.uncharted.software/WM/wm-request-queue

