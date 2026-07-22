BINARY ?= admin-gateway-sse
.PHONY: build run tidy
build:
	CGO_ENABLED=0 go build -o $(BINARY) .
tidy:
	go mod tidy
run: build
	./$(BINARY)
