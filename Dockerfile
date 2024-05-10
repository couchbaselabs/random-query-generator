FROM golang:alpine AS builder

RUN apk add --no-cache wget

WORKDIR /app

# Copy the entire project directory into the container
COPY . .

# Change directory to the cmd folder
WORKDIR /app/internal

# Download dependencies
RUN go mod download

# Build the Go application
RUN go build -o query_generator .

# Switch back to the root directory
WORKDIR /app

ENTRYPOINT ["/app/internal/query_generator"]