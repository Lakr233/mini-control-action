BINARY := minicontrol-runner

.PHONY: build test race vet lint docker clean

build:
	CGO_ENABLED=0 go build -trimpath -o bin/$(BINARY) ./cmd/$(BINARY)

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

lint: vet
	@command -v staticcheck >/dev/null && staticcheck ./... || echo "staticcheck not installed; skipped"
	@command -v shellcheck >/dev/null && shellcheck -s sh internal/bootstrap/scripts/bootstrap.sh.tmpl || echo "shellcheck not installed; skipped"

docker:
	docker build -t minicontrol-runner:latest .

clean:
	rm -rf bin
