FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o sldl-wrapper .

FROM debian:bookworm-slim
COPY --from=builder /app/sldl-wrapper /sldl-wrapper
COPY --from=builder /app/static /static
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libicu72 curl unzip && \
    curl -fsSL "https://github.com/fiso64/sldl/releases/download/v2.7.0-dev/sldl_linux-x64.zip" -o /tmp/sldl.zip && \
    unzip /tmp/sldl.zip sldl -d /sldl && \
    chmod +x /sldl/sldl && \
    rm -rf /tmp/sldl.zip /var/lib/apt/lists/*
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["/sldl-wrapper"]
