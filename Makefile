-include .env

.PHONY: run test fmt lint

run:
	go run cmd/orchestrator/main.go

test:
	go test ./... -race

fmt:
	go fmt ./...

lint:
	golangci-lint run
