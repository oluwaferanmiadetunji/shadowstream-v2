start_db:
	docker compose up --build -d

stop_db:
	docker compose down --volumes --remove-orphans

install:
	go mod tidy && go mod vendor

start:
	go run ./cmd/replicator/main.go

PHONY: start_db stop_db install start