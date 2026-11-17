package anthropic

import (
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

func newAdapter(url string) *Adapter {
	return New(Config{
		Name: "anthropic", BaseURL: url, APIKey: "ak",
		Client:      provider.NewHTTPClient(provider.TransportConfig{ResponseHeaderTimeout: time.Second}),
		IdleTimeout: 200 * time.Millisecond,
	})
}

func request() *api.ChatRequest {
	temp := 1.5
	return &api.ChatRequest{
		Model: "claude-haiku",
		Messages: []api.Message{
			{Role: "system", Content: "be brief"},
			{Role: "developer", Content: "no emojis"},
			{Role: "user", Content: "hi"},
			{Role: "assistant", Content: ""},
			{Role: "user", Content: "again"},
		},
		Temperature: &temp,
		Stop:        api.StopList{"END"},
		User:        "u1",
	}
}

func TestTranslatesRequest(t *testing.T) {
	up := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{Format: testutil.FormatAnthropic})
	req := request()
	before := request()
	s, err := newAdapter(up.URL).Open(t.Context(), "claude-haiku-4-5", req)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	var got map[string]any
	if err := json.Unmarshal(up.LastBody(), &got); err != nil {
		t.Fatal(err)
	}
	if got["model"] != "claude-haiku-4-5" || got["stream"] != true || got["max_tokens"] != float64(DefaultMaxTokens) {
		t.Fatalf("body = %s", up.LastBody())
	}
	if got["system"] != "be brief\n\nno emojis" {
		t.Fatalf("system = %v", got["system"])
	}
	msgs := got["messages"].([]any)
	if len(msgs) != 2 { // system messages lifted out, empty assistant dropped
		t.Fatalf("messages = %v", msgs)
	}
	if got["temperature"] != 1.0 {
		t.Fatalf("temperature not clamped: %v", got["temperature"])
	}
	if ss := got["stop_sequences"].([]any); ss[0] != "END" {
		t.Fatalf("stop_sequences = %v", ss)
	}
	if up.LastAPIKey() != "ak" || up.LastAuth() != "" {
		t.Fatal("anthropic uses x-api-key, not Authorization")
	}
	if !reflect.DeepEqual(req, before) {
		t.Fatal("adapter mutated the request")
	}
}

func TestTranslatesStream(t *testing.T) {
	up := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{Format: testutil.FormatAnthropic, Model: "claude-haiku-4-5-20251001"})
	s, err := newAdapter(up.URL).Open(t.Context(), "claude-haiku-4-5", request())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var text strings.Builder
	var chunks []*api.Chunk
	for {
		c, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, c)
		text.WriteString(c.Text())
	}
	if text.String() != "Hello, world" {
		t.Fatalf("text = %q", text.String())
	}
	first, last := chunks[0], chunks[len(chunks)-1]
	if first.Choices[0].Delta.Role != "assistant" || first.ID != "msg_fake" || first.Model != "claude-haiku-4-5-20251001" {
		t.Fatalf("first = %+v", first)
	}
	if last.Choices[0].FinishReason == nil || *last.Choices[0].FinishReason != "stop" {
		t.Fatalf("finish = %+v", last)
	}
	if last.Usage == nil || last.Usage.PromptTokens != 11 || last.Usage.CompletionTokens != 3 || last.Usage.TotalTokens != 14 {
		t.Fatalf("usage = %+v", last.Usage)
	}
	for _, c := range chunks {
		if c.Object != api.ObjectChunk {
			t.Fatalf("object = %q", c.Object)
		}
	}
}

func TestStatusClassification(t *testing.T) {
	tests := []struct {
		mode      testutil.Mode
		status    int
		retryable bool
	}{
		{testutil.Mode529, 529, true},
		{testutil.Mode429, 429, true},
		{testutil.Mode503, 503, true},
		{testutil.Mode400, 400, false},
		{testutil.Mode401, 401, false},
	}
	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			up := testutil.NewUpstream(t, tt.mode, testutil.Options{Format: testutil.FormatAnthropic})
			_, err := newAdapter(up.URL).Open(t.Context(), "m", request())
			var ue *provider.UpstreamError
			if !errors.As(err, &ue) || ue.Status != tt.status || ue.Retryable != tt.retryable {
				t.Fatalf("err = %#v", err)
			}
			if provider.IsRetryable(t.Context(), err) != tt.retryable {
				t.Fatal("IsRetryable disagrees with adapter classification")
			}
			if ue.Message == "" || strings.Contains(ue.Message, "{") {
				t.Fatalf("message not extracted: %q", ue.Message)
			}
		})
	}
}

func TestStreamFailures(t *testing.T) {
	t.Run("overloaded error event is retryable", func(t *testing.T) {
		up := testutil.NewUpstream(t, testutil.ModeErrorFirst, testutil.Options{Format: testutil.FormatAnthropic})
		s, err := newAdapter(up.URL).Open(t.Context(), "m", request())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		_, err = s.Next()
		var ue *provider.UpstreamError
		if !errors.As(err, &ue) || !ue.Retryable || ue.Status != 0 {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing message_stop is an error", func(t *testing.T) {
		up := testutil.NewUpstream(t, testutil.ModeNoDone, testutil.Options{Format: testutil.FormatAnthropic})
		s, err := newAdapter(up.URL).Open(t.Context(), "m", request())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		for {
			_, err = s.Next()
			if err != nil {
				break
			}
		}
		if !errors.Is(err, provider.ErrIncompleteStream) {
			t.Fatalf("err = %v, want ErrIncompleteStream", err)
		}
	})
}

func TestFinishReasonMapping(t *testing.T) {
	for in, want := range map[string]string{"end_turn": "stop", "stop_sequence": "stop", "max_tokens": "length", "tool_use": "tool_calls", "refusal": "content_filter"} {
		if got := finishReason(in); got != want {
			t.Errorf("finishReason(%q) = %q, want %q", in, got, want)
		}
	}
}
