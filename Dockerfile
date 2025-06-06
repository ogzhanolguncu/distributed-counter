FROM golang:1.23.5 as builder

WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o counter ./part4/cmd/counter/main.go

FROM debian:bullseye-slim
WORKDIR /app
COPY --from=builder /app/counter /app/
COPY ./start-nodes.sh /app/

RUN chmod +x /app/start-nodes.sh

EXPOSE 9000-9010 8010-8020
CMD ["/app/start-nodes.sh"]
