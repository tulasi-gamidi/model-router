package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/api"
	"github.com/tulasi-gamidi/model-router/internal/provider"
	"github.com/tulasi-gamidi/model-router/internal/provider/openai"
	"github.com/tulasi-gamidi/model-router/internal/router"
	"github.com/tulasi-gamidi/model-router/internal/sse"
	"github.com/tulasi-gamidi/model-router/internal/testutil"
)

const testKey = "gw-test-key"

// gateway starts the full stack (auth -> handler -> router -> openai adapter) in front of a
// primary (cheap) and backup (expensive) fake upstream.
func gateway(t *testing.T, primaryURL, backupURL string) *httptest.Server {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	mk := func(name, url string) provider.Provider {
		return openai.New(openai.Config{
			Name: name, BaseURL: url, APIKey: "upstream-key",
			Client:      provider.NewHTTPClient(provider.TransportConfig{ResponseHeaderTimeout: 300 * time.Millisecond}),
			IdleTimeout: 300 * time.Millisecond,
		})
	}
	m := router.Manifest{
		DefaultRoute: "auto",
		Models: map[string][]router.Target{
			"cheap":   {{Provider: "primary", UpstreamModel: "cheap-1", InputPerMTok: 0.1, OutputPerMTok: 0.2}},
			"premium": {{Provider: "backup", UpstreamModel: "premium-1", InputPerMTok: 3, OutputPerMTok: 15}},
		},
		Routes: map[string][]string{"auto": {"cheap", "premium"}},
	}
	rt, err := router.New(m, []provider.Provider{mk("primary", primaryURL), mk("backup", backupURL)}, log)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewMux(NewChatHandler(rt, log, 4096), NewAuth([]string{"other-key", testKey}, log)))
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, ctx context.Context, url, key, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

const okBody = `{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}`

type streamResult struct {
	text      string
	done      bool
	errEvents int
	chunks    []api.Chunk
}

func readSSE(t *testing.T, body io.Reader) streamResult {
	t.Helper()
	var res streamResult
	rd := sse.NewReader(body)
	for {
		ev, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return res
		}
		if err != nil {
			t.Fatalf("reading SSE: %v", err)
		}
		if res.done {
			t.Fatalf("event after [DONE]: %q", ev.Data)
		}
		if ev.Data == sse.DoneData {
			res.done = true
			continue
		}
		if strings.Contains(ev.Data, `"error"`) {
			res.errEvents++
			continue
		}
		var c api.Chunk
		if err := json.Unmarshal([]byte(ev.Data), &c); err != nil {
			t.Fatalf("bad chunk %q: %v", ev.Data, err)
		}
		res.chunks = append(res.chunks, c)
		res.text += c.Text()
	}
}

func decodeError(t *testing.T, resp *http.Response) api.ErrorBody {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	var e api.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	return e.Error
}

func TestChatHappyPath(t *testing.T) {
	primary := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	gw := gateway(t, primary.URL, backup.URL)

	resp := post(t, t.Context(), gw.URL, testKey, okBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	for k, want := range map[string]string{
		"Content-Type": "text/event-stream", "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
		"X-Router-Provider": "primary", "X-Router-Model": "cheap-1",
	} {
		if got := resp.Header.Get(k); got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}
	res := readSSE(t, resp.Body)
	if res.text != "Hello, world" || !res.done || res.errEvents != 0 {
		t.Fatalf("result = %+v", res)
	}
	last := res.chunks[len(res.chunks)-1]
	if last.Usage == nil || last.Usage.TotalTokens != 14 {
		t.Fatalf("usage chunk not forwarded: %+v", last)
	}
	if primary.LastAuth() != "Bearer upstream-key" {
		t.Fatal("gateway key must not be forwarded upstream; provider key must be used")
	}
	if backup.Called() {
		t.Fatal("backup should not be called")
	}
}

func TestChatFallbackIsInvisibleToClient(t *testing.T) {
	for _, mode := range []testutil.Mode{testutil.Mode429, testutil.Mode503, testutil.ModeStall, testutil.ModeErrorFirst} {
		t.Run(string(mode), func(t *testing.T) {
			primary := testutil.NewUpstream(t, mode, testutil.Options{})
			backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{Chunks: []string{"from ", "backup"}})
			gw := gateway(t, primary.URL, backup.URL)

			resp := post(t, t.Context(), gw.URL, testKey, okBody)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			res := readSSE(t, resp.Body)
			if res.text != "from backup" || !res.done || res.errEvents != 0 {
				t.Fatalf("result = %+v", res)
			}
			if !primary.Called() || resp.Header.Get("X-Router-Provider") != "backup" {
				t.Fatalf("primary called=%v provider=%q", primary.Called(), resp.Header.Get("X-Router-Provider"))
			}
		})
	}
}

func TestChatMidStreamFailure(t *testing.T) {
	for _, mode := range []testutil.Mode{testutil.ModeMidFail, testutil.ModeNoDone} {
		t.Run(string(mode), func(t *testing.T) {
			primary := testutil.NewUpstream(t, mode, testutil.Options{})
			backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
			gw := gateway(t, primary.URL, backup.URL)

			resp := post(t, t.Context(), gw.URL, testKey, okBody)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			res := readSSE(t, resp.Body)
			if res.text == "" || res.done || res.errEvents != 1 {
				t.Fatalf("want partial text + one error event + no [DONE], got %+v", res)
			}
			if backup.Called() {
				t.Fatal("no fallback after streaming started")
			}
		})
	}
}

func TestChatClientDisconnectAbortsUpstream(t *testing.T) {
	primary := testutil.NewUpstream(t, testutil.ModeHang, testutil.Options{})
	backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	gw := gateway(t, primary.URL, backup.URL)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	resp := post(t, ctx, gw.URL, testKey, okBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	// Read until the first content arrives, proving chunks are flushed before the stream ends.
	br := bufio.NewReader(resp.Body)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended before first content: %v", err)
		}
		if strings.Contains(line, `"content":"Hello"`) {
			break
		}
	}
	cancel() // client goes away mid-generation
	select {
	case <-primary.Cancelled():
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request was not cancelled after client disconnect")
	}
	if backup.Called() {
		t.Fatal("no fallback when the client disconnects")
	}
}

func TestChatAuth(t *testing.T) {
	tests := map[string]string{
		"missing":         "",
		"wrong":           "nope",
		"prefix of valid": testKey[:4],
		"valid plus junk": testKey + "x",
	}
	for name, key := range tests {
		t.Run(name, func(t *testing.T) {
			primary := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
			backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
			gw := gateway(t, primary.URL, backup.URL)
			resp := post(t, t.Context(), gw.URL, key, okBody)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			if e := decodeError(t, resp); e.Type != api.ErrTypeAuthentication {
				t.Fatalf("error = %+v", e)
			}
			if primary.Called() || backup.Called() {
				t.Fatal("upstream must not be called for an unauthenticated request")
			}
		})
	}
	t.Run("lowercase bearer scheme accepted", func(t *testing.T) {
		a := NewAuth([]string{testKey}, slog.New(slog.DiscardHandler))
		if !a.valid("bearer " + testKey) {
			t.Fatal("scheme should be case-insensitive")
		}
	})
}

func TestChatErrorsBeforeStreaming(t *testing.T) {
	tests := []struct {
		name        string
		primaryMode testutil.Mode
		backupMode  testutil.Mode
		body        string
		status      int
		errType     string
		upstream    bool // whether any upstream should have been called
	}{
		{"invalid json", testutil.ModeOK, testutil.ModeOK, `{"messages":`, 400, api.ErrTypeInvalidRequest, false},
		{"no messages", testutil.ModeOK, testutil.ModeOK, `{"model":"auto","messages":[]}`, 400, api.ErrTypeInvalidRequest, false},
		{"stream false", testutil.ModeOK, testutil.ModeOK, `{"stream":false,"messages":[{"role":"user","content":"x"}]}`, 400, api.ErrTypeInvalidRequest, false},
		{"unknown model", testutil.ModeOK, testutil.ModeOK, `{"model":"gpt-9","messages":[{"role":"user","content":"x"}]}`, 400, api.ErrTypeInvalidRequest, false},
		{"body too large", testutil.ModeOK, testutil.ModeOK, `{"messages":[{"role":"user","content":"` + strings.Repeat("a", 5000) + `"}]}`, 413, api.ErrTypeInvalidRequest, false},
		{"upstream 400 is not retried", testutil.Mode400, testutil.ModeOK, okBody, 400, api.ErrTypeInvalidRequest, true},
		{"upstream 401 is a gateway config error", testutil.Mode401, testutil.ModeOK, okBody, 502, api.ErrTypeUpstreamConfig, true},
		{"all targets fail", testutil.Mode429, testutil.Mode503, okBody, 502, api.ErrTypeNoAvailableProvider, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			primary := testutil.NewUpstream(t, tt.primaryMode, testutil.Options{})
			backup := testutil.NewUpstream(t, tt.backupMode, testutil.Options{})
			gw := gateway(t, primary.URL, backup.URL)
			resp := post(t, t.Context(), gw.URL, testKey, tt.body)
			if resp.StatusCode != tt.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.status)
			}
			e := decodeError(t, resp)
			if e.Type != tt.errType || e.Message == "" {
				t.Fatalf("error = %+v", e)
			}
			if primary.Called() != tt.upstream {
				t.Fatalf("primary called = %v, want %v", primary.Called(), tt.upstream)
			}
			if strings.Contains(e.Message, "upstream-key") {
				t.Fatal("error leaks provider key")
			}
		})
	}
}

func TestHealthzAndNotFound(t *testing.T) {
	primary := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	gw := gateway(t, primary.URL, primary.URL)
	resp, err := http.Get(gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d", resp.StatusCode)
	}
	resp2, err := http.Get(gw.URL + "/v1/embeddings")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp2.StatusCode)
	}
	resp3, err := http.Get(gw.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusMethodNotAllowed || resp3.Header.Get("Allow") != "POST" {
		t.Fatalf("GET chat: status = %d allow = %q", resp3.StatusCode, resp3.Header.Get("Allow"))
	}
	if primary.Called() {
		t.Fatal("upstream must not be called")
	}
}
