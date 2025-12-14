.PHONY: build run

build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/raft ./cmd/main.go

image: build
	docker build -t casual-raft:latest .