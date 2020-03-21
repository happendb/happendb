
proto-gen:
	protoc --proto_path=./proto/ -I ./proto/messaging/ -I ./proto/store/ --go_out=plugins=grpc,paths=source_relative:./proto/gen/go/ ./proto/store/*.proto