#!/bin/bash
FROM golang:latest
RUN mkdir /app
ADD main.go /app/
WORKDIR /app
RUN go get "github.com/micro/go-micro"
RUN go get "github.com/micro/go-micro/broker"
RUN go build -o=main main.go
RUN rm main.go
CMD [ "./main" ]
