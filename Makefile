-include .env

.PHONY: run test fmt lint

run:
	go run cmd/orchestrator/main.go

test:
	go test ./... -race

test-e2e:
	go test ./test/e2e/... -tags integration -v -count=1 -timeout 120s

fmt:
	go fmt ./...

lint:
	golangci-lint run
