#!/bin/sh

FROM golang:latest
RUN mkdir /app
ADD api /app/
WORKDIR /app
CMD [ "./api" ]