#!/bin/bash
FROM patreu22/goclean-base:latest
RUN mkdir /app
ADD main.go /app/
WORKDIR /app
RUN go build -o=main main.go
RUN rm main.go
CMD [ "./main" ]
