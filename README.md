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
| `11080` | SOCKS5 (WARP-backed) |
| `18080` | HTTP CONNECT / absolute-form proxy |
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
curl --socks5-hostname user:pass@127.0.0.1:11080 https://cloudflare.com/cdn-cgi/trace
curl -x http://user:pass@127.0.0.1:18080 https://cloudflare.com/cdn-cgi/trace
```

## Standalone (Windows attach)

```powershell
go build -o go-multi-warp.exe .
.\go-multi-warp.exe --config config.example.yaml
```

## Env

See `.env.example`. Important knobs:

| Env | Default | Notes |
|-----|---------|-------|
| `WARP_INSTANCES` / `MULTI_WARP_INSTANCES` | `10` | N warp-svc + backends |
| `MULTI_WARP_STRATEGY` | `round_robin` | `round_robin` / `least_conn` / `sticky` |
| `WARP_LICENSE_KEYS` | empty | **Comma-separated**. Instance `i` uses `keys[i % N]` |
| `MULTI_WARP_FORCE_REREGISTER` | `false` | Set `true` once after key change, redeploy, then set `false` |
| `MULTI_WARP_UNIQUE_IPV4` | `true` | Progressive admit + best-effort unique IPv4 expansion |
| `MULTI_WARP_UNIQUE_IPV4_MAX` | `20` | Effort only if `instances <= max` |
| `MULTI_WARP_UNIQUE_ATTEMPTS` | `8` | Re-reg attempts per slot after first |
| `MULTI_WARP_UNIQUE_STRICT` | `false` | `false`=admit non-unique after cap; `true`=park them |
| `MULTI_WARP_UNIQUE_BACKOFF_MS` | `8000` | Base re-reg backoff (exponential up to 8×, cap 60s) |
| `MULTI_WARP_UNIQUE_MAX_CONCURRENT_REG` | `2` | Max concurrent WARP re-registrations |
| `MULTI_WARP_UNIQUE_STAGGER_MS` | `3000` | Min gap between starting re-regs |
| `MULTI_WARP_UNIQUE_RECHECK_MS` | `60000` | Post-admit collision recheck interval |
| `MULTI_WARP_UNIQUE_RECHECK_MAX` | `3` | Max post-admit re-regs per backend |
| `PROXY_USER` / `PROXY_PASS` | `user`/`pass` | HTTP + SOCKS auth |
| `PROXY_MAX_CONN` | `1000` | per-IP connection cap |

### Progressive admit (serve ASAP)

1. First healthy backend is **admitted immediately** (proxy usable without waiting for all N).
2. Backends `2..N` (only if `instances <= 20`): probe egress IPv4; if not already in pool → admit; else re-register and retry (bounded).
3. Safer re-reg: concurrent cap + stagger + exponential backoff (less CF thrash).
4. Post-admit recheck: if two in-pool backends share IPv4, higher-ID one re-regs in background (capacity kept).
5. Admin honesty metrics: `in_pool`, `warming`, `parked`, `unique_ipv4`, `ceiling_hit`; `/metrics` has `unique` block (collisions, rereg_*, egress_histogram).

This improves **diversity best-effort**. It does **not** guarantee `instances` unique public IPv4 addresses — Cloudflare free WARP often shares a small egress pool per region. `in_pool≈N` with `unique_ipv4≈2–4` and `ceiling_hit=true` is expected success for free WARP mode C.

### Unique IPv4 (multi-key)

Free WARP registrations on one host usually share a tiny IPv4 egress pool. To target **N unique IPv4**:

1. Collect **N distinct WARP+ license keys** (one Cloudflare Zero Trust / WARP+ account key each, or N paid licenses).
2. Set Dokploy Environment:
   ```
   WARP_INSTANCES=10
   MULTI_WARP_INSTANCES=10
   MULTI_WARP_STRATEGY=round_robin
   WARP_LICENSE_KEYS=key1,key2,key3,key4,key5,key6,key7,key8,key9,key10
   MULTI_WARP_FORCE_REREGISTER=true
   ```
3. Redeploy once, confirm logs show `license applied` per instance id, then set `MULTI_WARP_FORCE_REREGISTER=false`.
4. Verify:
   ```bash
   for i in $(seq 1 20); do curl -sS -x http://user:pass@HOST:18080 https://api.ipify.org; echo; done | sort -u
   ```

Without keys (or with 1 shared free registration), multi-instance still works for concurrency but IPv4 uniqueness is **not guaranteed**.
