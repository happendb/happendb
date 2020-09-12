.PHONY: proto

build:
	go mod tidy
	env GOOS=linux go build -v -ldflags="-s -w" -o bin/client cmd/client/***
	env GOOS=linux go build -v -ldflags="-s -w" -o bin/server cmd/server/***
	chmod 0755 bin/* -v

proto:
	protoc --proto_path=./proto/ -I ./proto/happendb/messaging/ -I ./proto/happendb/store/ --go_out=plugins=grpc,paths=source_relative:./proto/gen/go/ ./proto/happendb/messaging/**/*.proto
	protoc --proto_path=./proto/ -I ./proto/happendb/messaging/ -I ./proto/happendb/store/ --go_out=plugins=grpc,paths=source_relative:./proto/gen/go/ ./proto/happendb/store/**/*.proto
	protoc --proto_path=./proto/ -I ./proto/happendb/messaging/ -I ./proto/happendb/store/ --grpc-gateway_out=logtostderr=true,paths=source_relative:./proto/gen/go/ ./proto/happendb/messaging/**/*.proto -I. --swagger_out=logtostderr=true:./proto/gen/swagger
	protoc --proto_path=./proto/ -I ./proto/happendb/messaging/ -I ./proto/happendb/store/ --grpc-gateway_out=logtostderr=true,paths=source_relative:./proto/gen/go/ ./proto/happendb/store/**/*.proto -I. --swagger_out=logtostderr=true:./proto/gen/swagger

test:
	go test -coverpkg=./... ./...