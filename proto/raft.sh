#!/bin/zsh
protoc --proto_path=. --go_out=. --go-grpc_out=. raft.proto