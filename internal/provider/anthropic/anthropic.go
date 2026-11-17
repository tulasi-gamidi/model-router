// Package anthropic adapts the Anthropic Messages API to the unified OpenAI-shaped schema.
//
// Request: system/developer messages move to the top-level "system" field, max_tokens becomes
// mandatory, temperature is clamped to Anthropic's 0..1 range, stop -> stop_sequences.
// Response: named SSE events (message_start, content_block_delta, message_delta, message_stop,
// error) are mapped to chat.completion.chunk objects; message_stop is the terminator.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/api"
	"github.com/tulasi-gamidi/model-router/internal/provider"
	"github.com/tulasi-gamidi/model-router/internal/sse"
)

// APIVersion is the anthropic-version header value.
const APIVersion = "2023-06-01"

// DefaultMaxTokens is used when the client does not set max_tokens (Anthropic requires it).
const DefaultMaxTokens = 1024

// StatusOverloaded is Anthropic's non-standard "overloaded" status.
const StatusOverloaded = 529

// Config configures an Adapter.
type Config struct {
	Name        string
	BaseURL     string // e.g. https://api.anthropic.com/v1
	APIKey      string
	Client      *http.Client
	IdleTimeout time.Duration
}

// Adapter implements provider.Provider for the Anthropic Messages API.
type Adapter struct {
	cfg Config
}

// New returns an Adapter.
func New(cfg Config) *Adapter {
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &Adapter{cfg: cfg}
}

// Name implements provider.Provider.
func (a *Adapter) Name() string { return a.cfg.Name }

type wireRequest struct {
	Model         string        `json:"model"`
	System        string        `json:"system,omitempty"`
	Messages      []wireMessage `json:"messages"`
	MaxTokens     int           `json:"max_tokens"`
	Stream        bool          `json:"stream"`
	Temperature   *float64      `json:"temperature,omitempty"`
	TopP          *float64      `json:"top_p,omitempty"`
	StopSequences []string      `json:"stop_sequences,omitempty"`
	Metadata      *metadata     `json:"metadata,omitempty"`
}

type wireMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type metadata struct {
	UserID string `json:"user_id"`
}

// buildRequest translates the unified request into a new Anthropic request; req is only read.
func buildRequest(model string, req *api.ChatRequest) wireRequest {
	out := wireRequest{Model: model, Stream: true, MaxTokens: DefaultMaxTokens}
	var system []string
	for _, m := range req.Messages {
		switch m.Role {
		case api.RoleSystem, api.RoleDeveloper:
			system = append(system, m.Content)
		case api.RoleAssistant:
			if m.Content != "" { // Anthropic rejects empty content blocks
				out.Messages = append(out.Messages, wireMessage{Role: "assistant", Content: m.Content})
			}
		default:
			out.Messages = append(out.Messages, wireMessage{Role: "user", Content: m.Content})
		}
	}
	out.System = strings.Join(system, "\n\n")
	if req.MaxTokens != nil {
		out.MaxTokens = *req.MaxTokens
	}
	if req.Temperature != nil {
		t := min(*req.Temperature, 1.0) // OpenAI allows 0..2, Anthropic 0..1
		out.Temperature = &t
	} else if req.TopP != nil {
		p := *req.TopP // Anthropic recommends setting only one of temperature/top_p
		out.TopP = &p
	}
	if len(req.Stop) > 0 {
		out.StopSequences = append([]string(nil), req.Stop...)
	}
	if req.User != "" {
		out.Metadata = &metadata{UserID: req.User}
	}
	return out
}

// Open implements provider.Provider.
func (a *Adapter) Open(ctx context.Context, model string, req *api.ChatRequest) (provider.Stream, error) {
	body, err := json.Marshal(buildRequest(model, req))
	if err != nil {
		return nil, fmt.Errorf("encoding request: %w", err)
	}
	tr := &translator{provider: a.cfg.Name, model: model}
	return provider.OpenSSE(ctx, provider.SSEOptions{
		Provider:        a.cfg.Name,
		Client:          a.cfg.Client,
		IdleTimeout:     a.cfg.IdleTimeout,
		RetryableStatus: retryableStatus,
		Translate:       tr.translate,
	}, func(ctx context.Context) (*http.Request, error) {
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.BaseURL+"/messages", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "text/event-stream")
		r.Header.Set("x-api-key", a.cfg.APIKey)
		r.Header.Set("anthropic-version", APIVersion)
		return r, nil
	})
}

func retryableStatus(status int) bool {
	return status == StatusOverloaded || status == http.StatusInternalServerError ||
		provider.DefaultRetryableStatus(status)
}

// translator holds per-stream state (message id, model, input tokens).
type translator struct {
	provider    string
	id          string
	model       string
	created     int64
	inputTokens int
}

type wireEvent struct {
	Type    string `json:"type"`
	Message struct {
		ID    string `json:"id"`
		Model string `json:"model"`
		Usage struct {
			InputTokens int `json:"input_tokens"`
		} `json:"usage"`
	} `json:"message"`
	Delta struct {
		Type       string `json:"type"`
		Text       string `json:"text"`
		StopReason string `json:"stop_reason"`
	} `json:"delta"`
	Usage struct {
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func (t *translator) chunk(delta api.Delta, finish *string) *api.Chunk {
	return &api.Chunk{
		ID: t.id, Object: api.ObjectChunk, Created: t.created, Model: t.model,
		Choices: []api.Choice{{Index: 0, Delta: delta, FinishReason: finish}},
	}
}

func (t *translator) translate(ev sse.Event) (*api.Chunk, bool, error) {
	var we wireEvent
	if err := json.Unmarshal([]byte(ev.Data), &we); err != nil {
		return nil, false, fmt.Errorf("%s: decoding event %q: %w", t.provider, ev.Name, err)
	}
	typ := we.Type
	if typ == "" {
		typ = ev.Name
	}
	switch typ {
	case "message_start":
		t.id = we.Message.ID
		if we.Message.Model != "" {
			t.model = we.Message.Model
		}
		t.created = time.Now().Unix()
		t.inputTokens = we.Message.Usage.InputTokens
		return t.chunk(api.Delta{Role: api.RoleAssistant}, nil), false, nil
	case "content_block_delta":
		if we.Delta.Type != "text_delta" || we.Delta.Text == "" {
			return nil, false, nil // thinking / tool-input deltas are out of scope
		}
		return t.chunk(api.Delta{Content: we.Delta.Text}, nil), false, nil
	case "message_delta":
		c := t.chunk(api.Delta{}, api.StrPtr(finishReason(we.Delta.StopReason)))
		c.Usage = &api.Usage{
			PromptTokens:     t.inputTokens,
			CompletionTokens: we.Usage.OutputTokens,
			TotalTokens:      t.inputTokens + we.Usage.OutputTokens,
		}
		return c, false, nil
	case "message_stop":
		return nil, true, nil
	case "error":
		return nil, false, &provider.UpstreamError{
			Provider:  t.provider,
			Retryable: retryableErrorType(we.Error.Type),
			Message:   we.Error.Message,
		}
	default: // ping, content_block_start, content_block_stop, unknown future events
		return nil, false, nil
	}
}

func finishReason(stop string) string {
	switch stop {
	case "max_tokens":
		return api.FinishLength
	case "tool_use":
		return "tool_calls"
	case "refusal":
		return "content_filter"
	default: // end_turn, stop_sequence, pause_turn
		return api.FinishStop
	}
}

func retryableErrorType(t string) bool {
	switch t {
	case "overloaded_error", "api_error", "rate_limit_error", "timeout_error":
		return true
	}
	return false
}
