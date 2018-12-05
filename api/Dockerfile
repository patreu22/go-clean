#!/bin/bash
FROM golang:latest
RUN mkdir /app
ADD rest.go /app/
WORKDIR /app
RUN go get "github.com/nats-io/go-nats"
RUN go build -o=api rest.go
RUN rm rest.go
CMD [ "./api" ]
