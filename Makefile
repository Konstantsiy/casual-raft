.PHONY: build run

build:
	CGO_ENABLED=0 go build -o bin/raft ./cmd/main.go

image: build
	docker build -t casual-raft:latest .

run:
	./bin/raft --id=1 --port=8001 --peers=localhost:8002,localhost:8003,localhost:8004,localhost:8005 --data=./data/node1
