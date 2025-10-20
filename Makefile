# compile ./internal/proto/raft.proto
raft-proto/compile:
	@protoc -I ./internal/proto \
	--go_out ./internal/proto/gen --go_opt=paths=source_relative \
	--go-grpc_out ./internal/proto/gen --go-grpc_opt=paths=source_relative \
	./internal/proto/raft.proto
