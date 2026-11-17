# model-router

[![CI](https://github.com/tulasi-gamidi/model-router/actions/workflows/ci.yml/badge.svg)](https://github.com/tulasi-gamidi/model-router/actions/workflows/ci.yml)

A small, production-minded LLM gateway in Go (standard library only). It exposes an OpenAI-compatible
`POST /v1/chat/completions`, routes each request to the **cheapest working** upstream model, relays the
response as **Server-Sent Events chunk by chunk** (no buffering), and **silently falls back** to the next
target when a provider fails transiently (429, 502/503/504, Anthropic 529, connect/header timeouts, or a
200 that stalls before its first token).

- Architecture, diagrams and failure handling: [architecture.md](architecture.md)
- Engineering rules used while building it: [CLAUDE.md](CLAUDE.md)

## Features

| Requirement | How |
|---|---|
| Unified API + schema translation | OpenAI chat format in and out. Adapters translate per vendor: `openai` (DigitalOcean / any OpenAI-compatible API) and `anthropic` (Messages API: system lifted out, `max_tokens` required, named events → chunks). |
| Streaming proxy (SSE) | Pull-based pipe: upstream event → translate → `data: {...}\n\n` → `Flush()`. One event in memory at a time; client backpressure propagates upstream. |
| Resilient fallback | Router tries targets in cost order and falls back until the first chunk is produced; after that the stream is committed. No fallback on 4xx or after client disconnect. |
| Cost-aware routing | Runtime cost manifest (`$/1M tokens` per target); aliases (`auto`, `fast`, `smart`) resolve to cheapest-first. Usage + estimated cost logged per request. |
| Authorization guard | `Authorization: Bearer <gateway key>` checked in middleware (constant-time) before any upstream work; 401 otherwise. |
| Observability | JSON `slog` lines per upstream attempt and per request (provider, model, status, fallback reason, ttfb, chunks, tokens, est. cost). `X-Request-Id`, `X-Router-Provider`, `X-Router-Model` response headers. Never logs keys or content. |

## Quick start

Requirements: Go 1.27+ (and `gcc` for `go test -race`).

```bash
git clone https://github.com/tulasi-gamidi/model-router.git && cd model-router
cp .env.example .env          # then fill in DO_MODEL_ACCESS_KEY and GATEWAY_API_KEYS
set -a && . ./.env && set +a
go run ./cmd/server           # listens on :8080
```

### API keys

| Variable | Required | Purpose |
|---|---|---|
| `DO_MODEL_ACCESS_KEY` | yes | DigitalOcean serverless inference key (Control panel → Serverless Inference → Model access keys). Used as the upstream key for the `digitalocean` provider. |
| `GATEWAY_API_KEYS` | yes | Comma-separated keys **your clients** must send (`Authorization: Bearer ...`). Min 8 chars each. Generate with `openssl rand -hex 24`. Never forwarded upstream. |
| `ANTHROPIC_API_KEY` | no | Enables the native Anthropic provider; models that list it (e.g. `claude-haiku`) gain a second real provider. |
| `DO_BASE_URL`, `ANTHROPIC_BASE_URL` | no | Override upstream base URLs. |
| `PORT` | no | Default `8080`. |
| `IDLE_TIMEOUT` | no | Max silence between upstream events, default `30s`. |
| `ROUTES_FILE` | no | Path to a custom routes/cost manifest (same shape as [routes.json](internal/config/routes.json)). |
| `ENABLE_DEMO_FALLBACK` | no | Registers an unreachable `demo-broken` provider so the `demo-fallback` route shows a live fallback. Keep `false` in production. |
| `LOG_LEVEL` | no | `debug` / `info` / `warn` / `error`. |

`.env` is gitignored. Invalid or missing configuration fails at startup with every problem listed.

### Models and routes

[routes.json](internal/config/routes.json) is the cost manifest (prices are illustrative config values, not billing data):

| `model` you send | Tries (cheapest first) |
|---|---|
| `auto` (default), `fast` | `mistral-3-14B` → `deepseek-3.2` → `qwen3.8-max` |
| `smart` | `deepseek-3.2` → `kimi-k2.6` → `qwen3.8-max` |
| `demo-fallback` | `demo-broken` (connection refused) → `mistral-3-14B` |
| any concrete model, e.g. `mistral-3-14B` | only providers of that model; never swapped for a different model |

## Sample requests

```bash
export GW=http://localhost:8080 KEY=<one of GATEWAY_API_KEYS>
```

**Streaming completion** (`-N` disables curl's buffering so chunks print as they arrive):

```bash
curl -N $GW/v1/chat/completions \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"model":"auto","stream":true,"messages":[{"role":"user","content":"Count from 1 to 10."}]}'
```
```text
data: {"id":"chatcmpl-…","object":"chat.completion.chunk","created":1794953805,"model":"mistral-3-14B","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}
data: {"id":"chatcmpl-…","object":"chat.completion.chunk","created":1794953805,"model":"mistral-3-14B","choices":[{"index":0,"delta":{"content":"1"},"finish_reason":null}]}
…
data: {"id":"chatcmpl-…","object":"chat.completion.chunk","created":1794953805,"model":"mistral-3-14B","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}
data: {"id":"chatcmpl-…","object":"chat.completion.chunk","created":1794953805,"model":"mistral-3-14B","choices":[],"usage":{"prompt_tokens":15,"completion_tokens":30,"total_tokens":45}}
data: [DONE]
```

**Live fallback** (needs `ENABLE_DEMO_FALLBACK=true`): the primary is unreachable, the client still gets a clean 200 stream:

```bash
curl -N -i $GW/v1/chat/completions -H "Authorization: Bearer $KEY" \
  -d '{"model":"demo-fallback","messages":[{"role":"user","content":"Say hello"}]}'
# HTTP/1.1 200 OK
# X-Router-Provider: digitalocean
# X-Router-Model: mistral-3-14B
```
Server log:
```json
{"level":"WARN","msg":"upstream attempt","provider":"demo-broken","attempt":1,"outcome":"failed","retryable":true,"error":"… connect: connection refused"}
{"level":"INFO","msg":"upstream attempt","provider":"digitalocean","model":"mistral-3-14B","attempt":2,"outcome":"ok","ttfb_ms":1062}
{"level":"INFO","msg":"chat request","route":"demo-fallback","provider":"digitalocean","outcome":"completed","chunks":14,"prompt_tokens":5,"completion_tokens":15,"est_cost_usd":0.000005}
```

**Rejected before any upstream work:**

```bash
curl -s $GW/v1/chat/completions -d '{"messages":[{"role":"user","content":"hi"}]}'
# 401 {"error":{"message":"invalid or missing API key","type":"authentication_error"}}

curl -s $GW/v1/chat/completions -H "Authorization: Bearer $KEY" -d '{"model":"nope","messages":[{"role":"user","content":"hi"}]}'
# 400 {"error":{"message":"unknown model \"nope\"","type":"invalid_request_error"}}
```

**Provider config error (no fallback):** a model your DO tier does not include:

```bash
curl -s $GW/v1/chat/completions -H "Authorization: Bearer $KEY" -d '{"model":"openai-gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
# 502 {"error":{"message":"provider digitalocean refused the gateway's credentials or model access (status 403)","type":"upstream_config_error"}}
```

**OpenAI SDKs work unchanged** (verified with the official `openai` Python package: streaming, usage,
fallback, and gateway errors surfacing as the SDK's typed `AuthenticationError` / `BadRequestError`):

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:8080/v1", api_key="<gateway key>")
stream = client.chat.completions.create(
    model="auto", stream=True, stream_options={"include_usage": True},
    messages=[{"role": "user", "content": "Name three colors."}])
for chunk in stream:
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
```

### Errors

All errors before streaming use the OpenAI shape `{"error":{"message","type"}}`:

| Status | `type` | When |
|---|---|---|
| 400 | `invalid_request_error` | bad JSON, failed validation, `stream:false`, unknown model, upstream rejected the request |
| 401 | `authentication_error` | missing/invalid gateway key |
| 404 | `invalid_request_error` | unknown path (non-chat endpoints are out of scope) |
| 405 | `invalid_request_error` | non-POST on `/v1/chat/completions` (`Allow: POST`) |
| 413 | `invalid_request_error` | body > 1MB |
| 502 | `upstream_config_error` | upstream 401/403/404 (gateway's provider key/tier/model id is wrong) |
| 502 | `no_available_provider` | every target failed with a retryable error |

After streaming has started, a failure is reported as one SSE event
`data: {"error":{"message":"upstream stream interrupted","type":"upstream_error"}}` and the stream ends
**without** `data: [DONE]`, so clients can tell a truncated answer from a complete one.

## Testing

```bash
gofmt -l . && go vet ./...     # must print nothing
go test -race ./...            # unit + integration, no network, no secrets
```

Tests never call real providers. `internal/testutil.NewUpstream` is a fake provider (`httptest.Server`)
speaking OpenAI or Anthropic wire format, with failure modes `400 401 429 503 529 slow stall error-first
midfail nodone hang`; it records whether it was called and whether it observed cancellation.

| Layer | Covers |
|---|---|
| `sse` | multi-line data, comments, CRLF, 1MB lines, oversize line, aborted connection |
| `provider` | `IsRetryable` table (incl. cancelled ctx wrapping a network error → not retryable) |
| `openai` adapter (+ shared `OpenSSE` plumbing) | request built correctly and not mutated, chunk/usage/finish_reason mapping, non-200 classification, missing `[DONE]`, mid-stream drop, stall → idle timeout (upstream sees cancel), vendor error event, client cancel unblocks `Next`, slow consumer is not idle, `Close` idempotent, header timeout, connection refused |
| `anthropic` adapter | request translation (system lifted, empty assistant dropped, temperature clamped, `x-api-key`), request not mutated, named events → chunks with usage/finish_reason, 529/429/503 retryable and 400/401 not, `overloaded_error` event retryable, missing `message_stop`. Timeouts/cancellation come from the same `OpenSSE` plumbing tested above. |
| `router` | cheapest first, fallback on 429/503/header timeout/stall/error-event/empty-200/conn-refused, no fallback on 400/401, all fail, client cancel stops immediately, concrete model never switches model, no fallback after first chunk, manifest validation |
| `httpapi` (full stack) | happy path + headers + usage, fallback invisible to client (200, backup text, no error event), mid-stream drop and missing `[DONE]` (partial + error event, no `[DONE]`), client disconnect cancels upstream, 401 never reaches upstream, error mapping |
| `config` | defaults, all-errors-at-once validation, embedded manifest compiles |

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs gofmt, vet, build and `go test -race` on every push and PR.

## Design decisions and trade-offs

- **Fallback window ends at the first substantive chunk, not at HTTP 200.** Providers often return 200 and
  then stall or emit an error event. The router peeks (buffering only role-only preamble chunks), so those
  cases still fall back invisibly. Once text has reached the client, switching models would splice two
  different answers together, so mid-stream failures are surfaced instead.
- **Retryability is decided by the adapter**, then read by one classifier. Vendors disagree (Anthropic's 529),
  and a global status list cannot capture that. Client cancellation is checked first, because a cancelled
  request often surfaces as a network error that would otherwise look retryable.
- **Upstream 401/403 does not fall back.** It is a gateway misconfiguration (bad key, model not in tier);
  failing loudly with `upstream_config_error` beats quietly shifting traffic to a pricier model. Same-model
  cross-provider fallback on 403 is a reasonable future policy knob.
- **Decode and re-encode chunks rather than piping raw bytes.** It costs a little CPU but guarantees the client
  only ever sees the unified schema (vendor extras like `reasoning_content` are dropped), and lets the gateway
  read `usage` for cost logging.
- **Idle timer only runs while waiting on the upstream.** A slow client applying backpressure must not be
  mistaken for a stalled provider.
- **`stream: false` is rejected** (400). This gateway is a streaming proxy; aggregation would be a thin
  wrapper over the same `Stream` and is listed below.
- **Single real provider caveat.** With only a DigitalOcean key, live fallback switches between models hosted
  on DO; it does not protect against a DO-wide outage. Adding `ANTHROPIC_API_KEY` (or any other
  OpenAI-compatible endpoint as another `openai` adapter instance) adds true provider redundancy with no code changes
  beyond wiring.

## Limitations and next steps

- Health-aware routing: per-target cooldown / circuit breaker using `Retry-After` and recent error rates,
  so a known-bad cheap target is skipped instead of being re-tried on every request.
- Key scopes: map each gateway key to allowed routes and a budget; per-key rate limiting.
- `stream:false` support by aggregating the same stream; OpenRouter-style `models: [...]` fallback list in the request.
- Tool calling and multimodal content (the `Provider` interface takes the whole request, so this is additive).
- Prometheus metrics (attempts, fallbacks, TTFB, tokens, cost) and OpenTelemetry tracing.
- Request id propagation into router attempt logs (currently on the per-request log line and response header).
- Dockerfile + deployment manifest.
