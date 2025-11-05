.PHONY: compile/raft-proto \
		test/sim-all test/sim-election test/sim-replication test/sim-persistennce test/sim-snapshots

# compile ./internal/proto/raft.proto
compile/raft-proto:
	@protoc -I ./internal/proto \
	--go_out ./internal/proto/gen --go_opt=paths=source_relative \
	--go-grpc_out ./internal/proto/gen --go-grpc_opt=paths=source_relative \
	./internal/proto/raft.proto

# run all simulation test
test/sim-all:
	@go test -v -race ./tests/
# run election tests (simulation)
test/sim-election:
	@go test -v -race ./tests/ -run 3A

# run replication tests (simulation)
test/sim-replication:
	@go test -v -race ./tests/ -run 3B

# run persistence tests (simulation)
test/sim-persistence:
	@go test -v -race ./tests/ -run 3C

# run snapshots tests (simulation)
test/sim-snapshots:
	@go test -v -race ./tests/ -run 3D
