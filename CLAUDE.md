# model-router

Go API gateway that exposes an OpenAI-compatible `POST /v1/chat/completions`, routes to real LLM
providers through adapters, streams responses back as SSE, and silently falls back to another
provider on transient errors.

## Commands
- Build: `go build ./...`
- Test: `go test -race ./...`
- Lint: `gofmt -l . && go vet ./...` (both must be clean)
- Run: `go run ./cmd/server` (reads config from env; see `.env.example`)

## How to work in this repo
- Work in small slices. Propose a short plan before writing a new component; wait for approval.
- Every change ships with tests. Run build, vet and `go test -race ./...` before saying a slice is done.
- Standard library only. Ask before adding any dependency (including provider SDKs).
- Never commit secrets. Keys come from env vars; `.env` is gitignored; update `.env.example` when adding a var.
- Keep the README and Mermaid diagrams in sync when behaviour changes.

## Go conventions
- Layout: `cmd/server` (wiring only), `internal/<pkg>` for everything else. No `pkg/`, no `utils`.
- Dependencies are passed in through struct fields / constructors (`NewX(...)`). No package-level mutable state.
- `context.Context` is the first parameter of any function that does I/O. Never store it in a struct.
  Documented exception: `Stream.Next()` takes no ctx; the stream is bound to the ctx given to `Open`
  (like an `http.Response` body) and keeps only its `CancelCauseFunc`.
- Errors: return them, wrap with `fmt.Errorf("doing x: %w", err)`, inspect with `errors.Is` / `errors.As`.
  No `panic` outside `main`. Don't log and return the same error.
- Keep interfaces small (1-3 methods).
- Logging: `log/slog`, structured key/value pairs. Never log API keys, auth headers, or prompt/response content.
- Always close response bodies (`defer resp.Body.Close()`), including on error paths.
- Every goroutine has a clear exit path tied to a context. Prefer no extra goroutines in the request path.
- HTTP server: Go 1.22 method routing (`mux.HandleFunc("POST /v1/...", h)`), `ReadHeaderTimeout` set,
  graceful shutdown via `signal.NotifyContext` + `srv.Shutdown(ctx)`.
- Request bodies are capped with `http.MaxBytesReader`. Invalid input → 400 with a JSON error.
- Error responses use the OpenAI shape: `{"error":{"message":"...","type":"..."}}`.
- Config from env vars, validated at startup; fail fast with a clear message.

## Streaming and fallback rules (critical)
- Unified schema = OpenAI chat completions format (text content only). Handler and Router never see vendor fields.
- Handler calls `Router.Route(ctx, req)`, which resolves `req.Model` to cost-ordered targets and calls
  `Provider.Open(ctx, target.UpstreamModel, req)` on each until one works.
- `req.Model` semantics: a route alias (`auto`, `fast`, ...) may fall back across models and providers; a
  concrete model falls back across providers of that model only (never silently swaps models); empty → default
  route; unknown → 400 before any upstream call.
- `Provider.Open(ctx, model, req)` sends the upstream request with `stream: true` and **checks the HTTP status
  before returning a Stream**. Non-200 → close body, return `*UpstreamError{Provider, Status, Retryable, Message}`.
- `Open` must not mutate `req` (it is reused across fallback attempts); adapters build a fresh vendor struct.
- Retryability is decided by the adapter (it knows its vendor): default 408/429/502/503/504; Anthropic adds 529
  and 500, plus `overloaded_error` events. The Router uses one classifier, `provider.IsRetryable(ctx, err)`, which
  checks `ctx.Err()` first (client gone → never retry), then `*UpstreamError.Retryable`, idle timeout,
  missing terminator, and transport errors (connect refused/reset, dial/TLS/header timeouts).
  Do NOT fall back on 400/401/403/other 4xx.
- Router peeks: after `Open` it reads until the first substantive chunk (role-only preamble chunks are buffered).
  Failures before that (200-then-stall, 200-then-error-event) still fall back. **Point of no return = first
  substantive chunk.** Handler writes status 200 and SSE headers only after `Route` returns; no fallback after.
- `Stream.Close()` is idempotent and safe after any error; the handler always `defer stream.Close()`.
- One goroutine per request, pull-based: `Next()` → translate → write `data: <json>\n\n` → `Flush()` after every chunk.
  Never buffer the full response, never `io.ReadAll` a successful stream body.
- Pass `r.Context()` to upstream requests (`http.NewRequestWithContext`) so client disconnect aborts upstream.
- End with `data: [DONE]\n\n`. On mid-stream failure, write one error event and stop **without** `[DONE]`.
- An upstream stream that ends without its terminator (`[DONE]` / `message_stop`) is an error, not success.
- SSE reader: `bufio.Scanner` with buffer raised to 1MB; ignore `:` comment lines; `scanner.Err()` means aborted stream.
- Response headers: `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`.

## Timeouts
- Connect 5s and TLS handshake 5s (`net.Dialer`, `Transport.TLSHandshakeTimeout`) → fallback.
- Response headers 30s (`Transport.ResponseHeaderTimeout`) → fallback.
- Idle between chunks 30s (`time.AfterFunc` that calls `cancel(ErrIdleTimeout)` on the stream's ctx). The timer
  is armed only while `Next()` waits on the upstream, so a slow client (backpressure) is never treated as idle.
  Before the first chunk → fallback; after → mid-stream error.
- Do NOT set `http.Client.Timeout` (it covers the whole body and kills long streams).
- Do NOT set `http.Server.WriteTimeout` (same reason).

## Auth
- Middleware checks `Authorization: Bearer <key>` against configured gateway keys before any upstream work.
- Compare keys with `crypto/subtle.ConstantTimeCompare` over SHA-256 digests (length-independent), checking every
  key without early exit. Reject with 401 JSON error.
- The gateway key is never forwarded upstream; each adapter uses its own provider key.

## Testing
- Table-driven tests; use `t.Helper()`, `t.Cleanup()`, `t.Context()` where available.
- Fake upstreams with `internal/testutil.NewUpstream` (`httptest.Server`; OpenAI or Anthropic wire format; modes:
  ok, 400, 401, 429, 503, 529, slow, stall, error-first, midfail, nodone, hang).
  The helper records whether it was called, the last body/auth headers, and whether the request context was cancelled.
- Never call real provider APIs in tests. CI has no secrets.
- Assert behaviour, not just `err == nil`: status code, streamed text, `[DONE]` present/absent,
  which provider served the request, upstream called/not called, cancellation observed.
- No `time.Sleep` for synchronisation; use channels, contexts, or short timeouts.
- Must-have cases: SSE parsing; each adapter's translation + non-200 + missing terminator;
  router fallback / no fallback on 400 / all fail / cost order; handler happy path, 429→fallback (200, backup text,
  no error event), mid-stream drop, client disconnect; auth 401 with upstream never called.

## Out of scope
- Non-chat endpoints (embeddings, images, audio, legacy `/v1/completions`).
- Multimodal content and tool calling (keep the adapter interface open for them).
- Automatic retries of the same provider (fallback goes to the next target instead).
