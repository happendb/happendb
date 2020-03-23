
proto-gen:
	protoc --proto_path=./proto/ -I ./proto/happendb/messaging/ -I ./proto/happendb/store/ --go_out=plugins=grpc,paths=source_relative:./proto/gen/go/ ./proto/happendb/messaging/**/*.proto
	protoc --proto_path=./proto/ -I ./proto/happendb/messaging/ -I ./proto/happendb/store/ --go_out=plugins=grpc,paths=source_relative:./proto/gen/go/ ./proto/happendb/store/**/*.proto