# Build proto

you should install protobuf first, and then generate grpc code

> go get -a github.com/golang/protobuf/protoc-gen-go
> protoc compute/pb/compute.proto  --go_out=plugins=grpc:.

# Run

> go run examples/wordcount/main.go
