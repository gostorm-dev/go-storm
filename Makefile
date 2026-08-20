VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS  = -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(DATE)

.PHONY: build clean install test race bench fmt vet

build:
	go build -ldflags "$(LDFLAGS)" -o storm ./cmd/storm

install:
	go install -ldflags "$(LDFLAGS)" ./cmd/storm

clean:
	rm -f storm

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -bench=. -benchmem ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

release:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o storm ./cmd/storm
