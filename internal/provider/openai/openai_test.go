package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/api"
	"github.com/tulasi-gamidi/model-router/internal/provider"
	"github.com/tulasi-gamidi/model-router/internal/testutil"
)

func newAdapter(t *testing.T, baseURL string, idle time.Duration) *Adapter {
	t.Helper()
	return New(Config{
		Name:        "fake-openai",
		BaseURL:     baseURL + "/",
		APIKey:      "secret-key",
		Client:      provider.NewHTTPClient(provider.TransportConfig{ResponseHeaderTimeout: 2 * time.Second}),
		IdleTimeout: idle,
	})
}

func sampleRequest() *api.ChatRequest {
	mt := 50
	temp := 0.2
	return &api.ChatRequest{
		Model:       "fast",
		Messages:    []api.Message{{Role: "system", Content: "be brief"}, {Role: "user", Content: "hi"}},
		MaxTokens:   &mt,
		Temperature: &temp,
		Stop:        api.StopList{"END"},
	}
}

// drain reads the stream to completion and returns the text, chunks and terminal error.
func drain(t *testing.T, s provider.Stream) (string, []*api.Chunk, error) {
	t.Helper()
	var sb strings.Builder
	var chunks []*api.Chunk
	for {
		c, err := s.Next()
		if err != nil {
			return sb.String(), chunks, err
		}
		chunks = append(chunks, c)
		sb.WriteString(c.Text())
	}
}

func TestOpenHappyPath(t *testing.T) {
	up := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	a := newAdapter(t, up.URL, time.Second)
	req := sampleRequest()
	before := sampleRequest()

	s, err := a.Open(t.Context(), "mistral-3-14B", req)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	text, chunks, err := drain(t, s)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("terminal err = %v, want io.EOF", err)
	}
	if text != "Hello, world" {
		t.Fatalf("text = %q", text)
	}
	if chunks[0].Choices[0].Delta.Role != "assistant" || chunks[0].Model != "mistral-3-14B" {
		t.Fatalf("first chunk = %+v", chunks[0])
	}
	var sawStop, sawUsage bool
	for _, c := range chunks {
		if len(c.Choices) > 0 && c.Choices[0].FinishReason != nil && *c.Choices[0].FinishReason == "stop" {
			sawStop = true
		}
		if c.Usage != nil && c.Usage.TotalTokens == 14 {
			sawUsage = true
		}
	}
	if !sawStop || !sawUsage {
		t.Fatalf("finish_reason seen=%v usage seen=%v", sawStop, sawUsage)
	}

	// Outbound request: concrete model, stream + usage requested, params forwarded, key sent.
	var sent map[string]any
	if err := json.Unmarshal(up.LastBody(), &sent); err != nil {
		t.Fatal(err)
	}
	if sent["model"] != "mistral-3-14B" || sent["stream"] != true || sent["max_tokens"] != float64(50) {
		t.Fatalf("sent body = %s", up.LastBody())
	}
	if so, _ := sent["stream_options"].(map[string]any); so["include_usage"] != true {
		t.Fatalf("include_usage not requested: %s", up.LastBody())
	}
	if up.LastAuth() != "Bearer secret-key" {
		t.Fatalf("auth header = %q", up.LastAuth())
	}
	if !reflect.DeepEqual(req, before) {
		t.Fatalf("adapter mutated the request: %+v", req)
	}
}

func TestOpenNon200(t *testing.T) {
	tests := []struct {
		mode       testutil.Mode
		status     int
		retryable  bool
		msg        string
		retryAfter time.Duration
	}{
		{testutil.Mode400, 400, false, "bad request from fake", 0},
		{testutil.Mode401, 401, false, "invalid api key", 0},
		{testutil.Mode429, 429, true, "rate limited", 7 * time.Second},
		{testutil.Mode503, 503, true, "unavailable", 0},
		{testutil.Mode529, 529, false, "Overloaded", 0}, // 529 is Anthropic-specific; not retryable by default here
	}
	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			up := testutil.NewUpstream(t, tt.mode, testutil.Options{})
			s, err := newAdapter(t, up.URL, time.Second).Open(t.Context(), "m", sampleRequest())
			if s != nil {
				t.Fatal("expected nil stream on non-200")
			}
			var ue *provider.UpstreamError
			if !errors.As(err, &ue) {
				t.Fatalf("err = %v, want *UpstreamError", err)
			}
			if ue.Status != tt.status || ue.Retryable != tt.retryable || ue.Message != tt.msg || ue.RetryAfter != tt.retryAfter {
				t.Fatalf("got %+v", ue)
			}
		})
	}
}

func TestStreamFailures(t *testing.T) {
	tests := []struct {
		name      string
		mode      testutil.Mode
		wantText  string
		check     func(error) bool
		retryable bool
	}{
		{"missing [DONE] is an error", testutil.ModeNoDone, "Hello, world",
			func(err error) bool { return errors.Is(err, provider.ErrIncompleteStream) }, true},
		{"connection dropped mid-stream", testutil.ModeMidFail, "Hello",
			func(err error) bool { return err != nil && !errors.Is(err, io.EOF) }, true},
		{"stall after 200 hits idle timeout", testutil.ModeStall, "",
			func(err error) bool { return errors.Is(err, provider.ErrIdleTimeout) }, true},
		{"vendor error event", testutil.ModeErrorFirst, "",
			func(err error) bool {
				var ue *provider.UpstreamError
				return errors.As(err, &ue) && ue.Status == 0 && ue.Message == "server overloaded"
			}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := testutil.NewUpstream(t, tt.mode, testutil.Options{})
			s, err := newAdapter(t, up.URL, 100*time.Millisecond).Open(t.Context(), "m", sampleRequest())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			text, _, err := drain(t, s)
			if text != tt.wantText {
				t.Errorf("text = %q, want %q", text, tt.wantText)
			}
			if !tt.check(err) {
				t.Fatalf("unexpected terminal error: %v", err)
			}
			if got := provider.IsRetryable(t.Context(), err); got != tt.retryable {
				t.Fatalf("IsRetryable = %v, want %v", got, tt.retryable)
			}
			// Errors are sticky.
			if _, err2 := s.Next(); err2 == nil {
				t.Fatal("Next after failure returned nil error")
			}
		})
	}
	t.Run("idle timeout aborts the upstream request", func(t *testing.T) {
		up := testutil.NewUpstream(t, testutil.ModeStall, testutil.Options{})
		s, err := newAdapter(t, up.URL, 50*time.Millisecond).Open(t.Context(), "m", sampleRequest())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		_, _ = s.Next()
		select {
		case <-up.Cancelled():
		case <-time.After(2 * time.Second):
			t.Fatal("upstream never observed cancellation")
		}
	})
}

func TestClientCancelUnblocksNext(t *testing.T) {
	up := testutil.NewUpstream(t, testutil.ModeHang, testutil.Options{})
	ctx, cancel := context.WithCancel(t.Context())
	s, err := newAdapter(t, up.URL, 10*time.Second).Open(ctx, "m", sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Next(); err != nil { // role chunk
		t.Fatal(err)
	}
	if c, err := s.Next(); err != nil || c.Text() != "Hello" {
		t.Fatalf("got %v, %v", c, err)
	}
	cancel()
	_, err = s.Next()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if provider.IsRetryable(ctx, err) {
		t.Fatal("cancelled stream must not be retryable")
	}
	select {
	case <-up.Cancelled():
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never observed cancellation")
	}
}

func TestSlowConsumerIsNotIdle(t *testing.T) {
	up := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	idle := 50 * time.Millisecond
	s, err := newAdapter(t, up.URL, idle).Open(t.Context(), "m", sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Next(); err != nil {
		t.Fatal(err)
	}
	// Simulate a client applying backpressure for longer than the idle timeout. This is a
	// deliberate delay under test, not synchronisation.
	<-time.After(4 * idle)
	text, _, err := drain(t, s)
	if !errors.Is(err, io.EOF) || text != "Hello, world" {
		t.Fatalf("text=%q err=%v: backpressure was treated as upstream idleness", text, err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	up := testutil.NewUpstream(t, testutil.ModeHang, testutil.Options{})
	s, err := newAdapter(t, up.URL, time.Second).Open(t.Context(), "m", sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next(); err == nil {
		t.Fatal("Next after Close should fail")
	}
}

func TestResponseHeaderTimeoutIsRetryable(t *testing.T) {
	up := testutil.NewUpstream(t, testutil.ModeSlow, testutil.Options{})
	a := New(Config{
		Name: "slow", BaseURL: up.URL, APIKey: "k",
		Client: provider.NewHTTPClient(provider.TransportConfig{ResponseHeaderTimeout: 50 * time.Millisecond}),
	})
	_, err := a.Open(t.Context(), "m", sampleRequest())
	if err == nil {
		t.Fatal("expected header timeout error")
	}
	if !provider.IsRetryable(t.Context(), err) {
		t.Fatalf("header timeout should be retryable: %v", err)
	}
}

func TestConnectionRefusedIsRetryable(t *testing.T) {
	a := New(Config{Name: "down", BaseURL: "http://127.0.0.1:1", APIKey: "k",
		Client: provider.NewHTTPClient(provider.TransportConfig{})})
	_, err := a.Open(t.Context(), "m", sampleRequest())
	if err == nil || !provider.IsRetryable(t.Context(), err) {
		t.Fatalf("err = %v, want retryable", err)
	}
}
