#!/bin/bash
FROM patreu22/goclean-base:latest
RUN mkdir /app
ADD rest.go /app/
WORKDIR /app
RUN go build -o=api rest.go
RUN rm rest.go
CMD [ "./api" ]
