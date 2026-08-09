.PHONY: build test lint fmt vet clean

build:
	go build -o go-storm ./cmd/storm

test:
	go test ./... -race -cover

lint:
	go vet ./...

fmt:
	gofmt -l -w .

vet:
	go vet ./...

clean:
	rm -f go-storm
