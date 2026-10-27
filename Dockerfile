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
        python3-mutagen python3-requests python3-venv && \
    rm -rf /var/lib/apt/lists/*
# beets from PyPI, not apt: bookworm ships beets 1.6.0, which stores albumtypes
# as one "album; live" string that mediafile then writes char-by-char
# (MusicBrainz Album Type = a,l,b,u,m). Fixed in beets 2.x.
RUN python3 -m venv /opt/beets && \
    /opt/beets/bin/pip install --no-cache-dir "beets==2.3.1" requests && \
    ln -s /opt/beets/bin/beet /usr/local/bin/beet
COPY --from=builder /app/sldl-wrapper /sldl-wrapper
COPY --from=builder /app/static /static
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["/sldl-wrapper"]
