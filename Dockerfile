FROM golang:1.23.5-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build all services
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/discovery-server ./part3/cmd/discovery/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/counter-node ./part3/cmd/counter/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/gateway ./part3/cmd/gateway/main.go

# Create a minimal runtime image
FROM alpine:latest

RUN apk --no-cache add ca-certificates

COPY --from=builder /bin/discovery-server /bin/discovery-server
COPY --from=builder /bin/counter-node /bin/counter-node
COPY --from=builder /bin/gateway /bin/gateway

# Create directories for data persistence
RUN mkdir -p /data/wal

# Set environment variables
ENV DISCOVERY_ADDR=discovery:8000

# Default command (will be overridden in docker-compose)
CMD ["echo", "Specify either discovery-server or counter-node"]
