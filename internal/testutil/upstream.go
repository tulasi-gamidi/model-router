// Package testutil provides a fake LLM upstream for adapter, router and handler tests.
// Real provider APIs are never called from tests.
package testutil

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

// Mode selects how the fake upstream behaves.
type Mode string

const (
	ModeOK         Mode = "ok"          // stream all chunks and the terminator
	Mode400        Mode = "400"         // 400 invalid request
	Mode401        Mode = "401"         // 401 bad key
	Mode429        Mode = "429"         // 429 with Retry-After
	Mode503        Mode = "503"         // 503 unavailable
	Mode529        Mode = "529"         // Anthropic overloaded
	ModeSlow       Mode = "slow"        // never sends response headers (header timeout / cancel)
	ModeStall      Mode = "stall"       // 200 + headers, then silence until cancelled
	ModeErrorFirst Mode = "error-first" // 200, then a retryable vendor error event as the first event
	ModeMidFail    Mode = "midfail"     // some chunks, then the connection is aborted
	ModeNoDone     Mode = "nodone"      // all chunks, clean end, but no terminator
	ModeHang       Mode = "hang"        // first chunk, then silence until cancelled (client disconnect tests)
)

// Format selects the wire format of the fake.
type Format string

const (
	FormatOpenAI    Format = "openai"
	FormatAnthropic Format = "anthropic"
)

// DefaultChunks is the text streamed when Options.Chunks is empty.
var DefaultChunks = []string{"Hello", ", ", "world"}

// Options configure a fake upstream.
type Options struct {
	Format Format   // default FormatOpenAI
	Chunks []string // default DefaultChunks
	Model  string   // model reported in chunks; default echoes the request model
}

// Upstream is a fake provider endpoint backed by httptest.Server.
type Upstream struct {
	*httptest.Server
	mode Mode
	opts Options

	calls     atomic.Int32
	cancelled chan struct{}
	cancelOne sync.Once
	stop      chan struct{} // closed at cleanup so blocked handlers exit

	mu       sync.Mutex
	lastBody []byte
	lastAuth string
	lastKey  string
}

// NewUpstream starts a fake upstream that is closed automatically at test cleanup.
func NewUpstream(t *testing.T, mode Mode, opts Options) *Upstream {
	t.Helper()
	if opts.Format == "" {
		opts.Format = FormatOpenAI
	}
	if len(opts.Chunks) == 0 {
		opts.Chunks = DefaultChunks
	}
	u := &Upstream{mode: mode, opts: opts, cancelled: make(chan struct{}), stop: make(chan struct{})}
	u.Server = httptest.NewServer(http.HandlerFunc(u.serve))
	t.Cleanup(u.Server.Close)
	t.Cleanup(func() { close(u.stop) }) // runs before Server.Close (LIFO)
	return u
}

// Calls reports how many requests the fake received.
func (u *Upstream) Calls() int { return int(u.calls.Load()) }

// Called reports whether the fake received any request.
func (u *Upstream) Called() bool { return u.Calls() > 0 }

// Cancelled is closed once the fake observes a request context being cancelled
// while it was still serving.
func (u *Upstream) Cancelled() <-chan struct{} { return u.cancelled }

// LastBody returns the body of the most recent request.
func (u *Upstream) LastBody() []byte {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastBody
}

// LastAuth returns the Authorization header of the most recent request.
func (u *Upstream) LastAuth() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastAuth
}

// LastAPIKey returns the x-api-key header of the most recent request.
func (u *Upstream) LastAPIKey() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastKey
}

func (u *Upstream) markCancelled() { u.cancelOne.Do(func() { close(u.cancelled) }) }

// waitCancel blocks until the request is cancelled or the test ends.
func (u *Upstream) waitCancel(r *http.Request) {
	select {
	case <-r.Context().Done():
		u.markCancelled()
	case <-u.stop:
	}
}

func (u *Upstream) serve(w http.ResponseWriter, r *http.Request) {
	u.calls.Add(1)
	body, _ := io.ReadAll(r.Body)
	var reqModel struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &reqModel)
	u.mu.Lock()
	u.lastBody, u.lastAuth, u.lastKey = body, r.Header.Get("Authorization"), r.Header.Get("x-api-key")
	u.mu.Unlock()

	model := u.opts.Model
	if model == "" {
		model = reqModel.Model
	}

	switch u.mode {
	case Mode400:
		u.writeError(w, http.StatusBadRequest, "invalid_request_error", "bad request from fake")
		return
	case Mode401:
		u.writeError(w, http.StatusUnauthorized, "authentication_error", "invalid api key")
		return
	case Mode429:
		w.Header().Set("Retry-After", "7")
		u.writeError(w, http.StatusTooManyRequests, "rate_limit_error", "rate limited")
		return
	case Mode503:
		u.writeError(w, http.StatusServiceUnavailable, "server_error", "unavailable")
		return
	case Mode529:
		u.writeError(w, 529, "overloaded_error", "Overloaded")
		return
	case ModeSlow:
		u.waitCancel(r)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher := w.(http.Flusher)
	flusher.Flush()
	ev := &eventWriter{w: w, f: flusher, format: u.opts.Format, model: model}

	switch u.mode {
	case ModeStall:
		u.waitCancel(r)
		return
	case ModeErrorFirst:
		ev.error("overloaded_error", "server overloaded")
		return
	}

	ev.start()
	chunks := u.opts.Chunks
	if u.mode == ModeMidFail || u.mode == ModeHang {
		chunks = chunks[:1]
	}
	for _, c := range chunks {
		ev.text(c)
	}
	switch u.mode {
	case ModeMidFail:
		panic(http.ErrAbortHandler) // abort the connection without a clean chunked end
	case ModeHang:
		u.waitCancel(r)
		return
	case ModeNoDone:
		return
	}
	ev.finish()
}

func (u *Upstream) writeError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if u.opts.Format == FormatAnthropic {
		fmt.Fprintf(w, `{"type":"error","error":{"type":%q,"message":%q}}`, typ, msg)
		return
	}
	fmt.Fprintf(w, `{"error":{"type":%q,"message":%q}}`, typ, msg)
}

type eventWriter struct {
	w      io.Writer
	f      http.Flusher
	format Format
	model  string
}

func (e *eventWriter) send(name string, v any) {
	b, _ := json.Marshal(v)
	if name != "" {
		fmt.Fprintf(e.w, "event: %s\n", name)
	}
	fmt.Fprintf(e.w, "data: %s\n\n", b)
	e.f.Flush()
}

func (e *eventWriter) openaiChunk(delta map[string]any, finish any, usage any) map[string]any {
	c := map[string]any{
		"id": "chatcmpl-fake", "object": "chat.completion.chunk", "created": 1, "model": e.model,
		"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}},
	}
	if usage != nil {
		c["choices"] = []any{}
		c["usage"] = usage
	}
	return c
}

func (e *eventWriter) start() {
	if e.format == FormatAnthropic {
		e.send("message_start", map[string]any{"type": "message_start", "message": map[string]any{
			"id": "msg_fake", "model": e.model, "usage": map[string]any{"input_tokens": 11, "output_tokens": 1}}})
		fmt.Fprint(e.w, "event: ping\ndata: {\"type\":\"ping\"}\n\n")
		e.send("content_block_start", map[string]any{"type": "content_block_start", "index": 0,
			"content_block": map[string]any{"type": "text", "text": ""}})
		return
	}
	fmt.Fprint(e.w, ": keep-alive comment\n\n")
	e.send("", e.openaiChunk(map[string]any{"role": "assistant", "content": ""}, nil, nil))
}

func (e *eventWriter) text(s string) {
	if e.format == FormatAnthropic {
		e.send("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0,
			"delta": map[string]any{"type": "text_delta", "text": s}})
		return
	}
	e.send("", e.openaiChunk(map[string]any{"content": s, "reasoning_content": "ignored"}, nil, nil))
}

func (e *eventWriter) finish() {
	if e.format == FormatAnthropic {
		e.send("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
		e.send("message_delta", map[string]any{"type": "message_delta",
			"delta": map[string]any{"stop_reason": "end_turn"}, "usage": map[string]any{"output_tokens": 3}})
		e.send("message_stop", map[string]any{"type": "message_stop"})
		return
	}
	e.send("", e.openaiChunk(map[string]any{}, "stop", nil))
	e.send("", e.openaiChunk(nil, nil, map[string]any{"prompt_tokens": 11, "completion_tokens": 3, "total_tokens": 14}))
	fmt.Fprint(e.w, "data: [DONE]\n\n")
	e.f.Flush()
}

func (e *eventWriter) error(typ, msg string) {
	if e.format == FormatAnthropic {
		e.send("error", map[string]any{"type": "error", "error": map[string]any{"type": typ, "message": msg}})
		return
	}
	e.send("", map[string]any{"error": map[string]any{"type": typ, "message": msg}})
}
