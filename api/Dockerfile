#!/bin/bash
FROM golang:latest
RUN mkdir /app
ADD rest.go /app/
WORKDIR /app
RUN go get "github.com/emicklei/go-restful"
RUN go get "github.com/micro/go-micro/broker"
RUN go get "github.com/micro/go-plugins/broker/nats"
RUN go get "github.com/micro/go-web"
RUN go build -o=api rest.go
RUN rm rest.go
CMD [ "./api" ]
