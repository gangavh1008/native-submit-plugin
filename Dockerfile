FROM golang:1.23-alpine as builder
WORKDIR /app
COPY . .
RUN go build -o native-submit ./main

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/native-submit .
ENTRYPOINT ["/app/native-submit"] 