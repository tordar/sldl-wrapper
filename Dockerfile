FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o sldl-wrapper .

FROM debian:bookworm-slim
# System packages come before the app COPYs so code changes reuse this cached
# layer. sldl itself is not baked in: docker-compose mounts it at /sldl/sldl.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libicu72 \
        beets python3-mutagen python3-requests && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/sldl-wrapper /sldl-wrapper
COPY --from=builder /app/static /static
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["/sldl-wrapper"]
