FROM golang:1.8

WORKDIR /go/src/github.com/naturali/kmr
COPY . .

EXPOSE 50051


