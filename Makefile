test:
	go test ./...

generate:
	go generate ./...

mocks:
	mockery

.PHONY: test generate mocks
