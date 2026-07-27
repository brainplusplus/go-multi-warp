# go-multi-warp

Production multi-instance Cloudflare WARP manager + high-RPS HTTP/SOCKS5 load balancer (Go).

| Problem (bash multi-instance) | go-multi-warp |
|---|---|
| Health snapshot only at boot | Continuous probes every N ms |
| Dead backends still in RR | Circuit breaker + least_conn |
| No reconnect after drop | Soft reconnect + hard restart supervisor |
| PROXY_MAX_CONN=10 kills farms | Global / per-IP high limits |
| HTTP & SOCKS diverge | Shared pool for both |

## Endpoints

| Port | Protocol |
|------|----------|
| `1080` | SOCKS5 (WARP-backed) |
| `8080` | HTTP CONNECT / absolute-form proxy |
| `9090` | Admin: `/healthz` `/readyz` `/metrics` `/backends` |

## Modes

### `managed` (Linux container / VPS)
Spawns N× `warp-svc` with isolated state dirs, registers, sets proxy mode, probes, reconnects.

### `attach` (Windows / any host)
Does not spawn WARP. Balances across existing SOCKS backends at `base_port + i` or `backends:`.

## Docker

```bash
docker compose up -d --build

curl http://127.0.0.1:9090/readyz
curl --socks5-hostname user:pass@127.0.0.1:1080 https://cloudflare.com/cdn-cgi/trace
curl -x http://user:pass@127.0.0.1:8080 https://cloudflare.com/cdn-cgi/trace
```

## Standalone (Windows attach)

```powershell
go build -o go-multi-warp.exe .
.\go-multi-warp.exe --config config.example.yaml
```

## Env

- `WARP_INSTANCES` / `MULTI_WARP_INSTANCES`
- `MULTI_WARP_MODE=managed|attach`
- `PROXY_USER` / `PROXY_PASS` / `PROXY_MAX_CONN`
- `WARP_LICENSE_KEY` (comma-separated)
- `WARP_ORG` + `WARP_AUTH_CLIENT_ID` + `WARP_AUTH_CLIENT_SECRET`
- `MULTI_WARP_BACKENDS=127.0.0.1:40000,127.0.0.1:40001`
