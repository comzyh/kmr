# build proto

you should install protobuf first, and then generate

> go get -a github.com/golang/protobuf/protoc-gen-go
> protoc compute/pb/compute.proto  --go_out=plugins=grpc:.
