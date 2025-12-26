start_docker:
	docker compose up --build

stop_docker:
	docker compose down --volumes --remove-orphans

install:
	go mod tidy && go mod vendor

start:
	go run ./cmd/replicator/main.go

PHONY: start_docker stop_docker install start