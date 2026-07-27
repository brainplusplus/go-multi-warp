# syntax=docker/dockerfile:1.6
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o go-multi-warp .

FROM ubuntu:24.04 AS runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg lsb-release sudo dbus jq \
    && curl -fsSL https://pkg.cloudflareclient.com/pubkey.gpg \
      | gpg --yes --dearmor --output /usr/share/keyrings/cloudflare-warp-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloudflare-warp-archive-keyring.gpg] https://pkg.cloudflareclient.com/ $(lsb_release -cs) main" \
      > /etc/apt/sources.list.d/cloudflare-client.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends cloudflare-warp \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && useradd -m -s /bin/bash warp \
    && echo "warp ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/warp \
    && mkdir -p /home/warp/.local/share/warp /etc/multi-warp /var/lib/cloudflare-warp /run/multi-warp \
    && echo -n yes > /home/warp/.local/share/warp/accepted-tos.txt \
    && chown -R warp:warp /home/warp /etc/multi-warp /var/lib/cloudflare-warp /run/multi-warp

COPY --from=builder /src/go-multi-warp /usr/local/bin/go-multi-warp
COPY config.docker.yaml /etc/multi-warp/config.yaml
RUN chmod +x /usr/local/bin/go-multi-warp

# warp-svc needs to write into data dirs; run as root for managed mode reliability
WORKDIR /root

ENV MULTI_WARP_MODE=managed
ENV WARP_INSTANCES=5
ENV MULTI_WARP_INSTANCES=5
ENV PROXY_USER=user
ENV PROXY_PASS=pass
ENV PROXY_MAX_CONN=500
ENV RUST_LOG=info

EXPOSE 1080 8080 9090

HEALTHCHECK --interval=15s --timeout=5s --start-period=180s --retries=5 \
  CMD curl -fsS http://127.0.0.1:9090/healthz || exit 1

ENTRYPOINT ["go-multi-warp", "--config", "/etc/multi-warp/config.yaml"]
