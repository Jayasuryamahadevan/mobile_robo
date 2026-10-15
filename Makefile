.PHONY: test fmt up down
test:
	go test ./...
fmt:
	gofmt -w ./cmd ./internal
up:
	docker compose up --build -d
down:
	docker compose down
