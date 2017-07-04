FROM golang:1.8

RUN apt-get -y update
RUN apt-get -y install librados-dev

WORKDIR /go/src/github.com/naturali/kmr
COPY . .

EXPOSE 50051
