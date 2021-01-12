.PHONY: init
init:
	go get -u google.golang.org/protobuf/proto
	go get -u github.com/golang/protobuf/protoc-gen-go

.PHONY: proto
proto:
	protoc --proto_path=./proto/ -I ./proto/happendb/messaging/ -I ./proto/happendb/store/ --go_out=plugins=grpc,paths=source_relative:./proto/gen/go/ ./proto/happendb/messaging/**/*.proto
	protoc --proto_path=./proto/ -I ./proto/happendb/messaging/ -I ./proto/happendb/store/ --go_out=plugins=grpc,paths=source_relative:./proto/gen/go/ ./proto/happendb/store/**/*.proto

.PHONY: build
build:
	go mod tidy
	env GOOS=linux go build -v -ldflags="-s -w" -o bin/client cmd/client/***
	env GOOS=linux go build -v -ldflags="-s -w" -o bin/server cmd/server/***
	chmod 0755 bin/* -v

.PHONY: test
test:
	go test -v ./... cover
