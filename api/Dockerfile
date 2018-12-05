#!/bin/bash
FROM goclean-base
RUN mkdir /app
ADD rest.go /app/
WORKDIR /app
RUN go build -o=api rest.go
RUN rm rest.go
CMD [ "./api" ]
