// Package openai is the adapter for OpenAI-compatible chat completion APIs (DigitalOcean
// serverless inference, OpenAI, vLLM, ...). The unified schema is already OpenAI-shaped, so
// translation is close to a pass-through, but vendor-only fields (e.g. reasoning_content) are
// dropped and the request is rebuilt rather than forwarded.
package openai

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

// Config configures an Adapter.
type Config struct {
	Name        string // provider name, e.g. "digitalocean"
	BaseURL     string // e.g. https://inference.do-ai.run/v1 (no trailing /chat/completions)
	APIKey      string
	Client      *http.Client
	IdleTimeout time.Duration
}

// Adapter implements provider.Provider for OpenAI-compatible endpoints.
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
	Messages      []wireMessage `json:"messages"`
	Stream        bool          `json:"stream"`
	StreamOptions streamOptions `json:"stream_options"`
	MaxTokens     *int          `json:"max_tokens,omitempty"`
	Temperature   *float64      `json:"temperature,omitempty"`
	TopP          *float64      `json:"top_p,omitempty"`
	Stop          []string      `json:"stop,omitempty"`
	User          string        `json:"user,omitempty"`
}

type wireMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// buildRequest creates a fresh wire request; req is only read.
func buildRequest(model string, req *api.ChatRequest) wireRequest {
	msgs := make([]wireMessage, len(req.Messages))
	for i, m := range req.Messages {
		msgs[i] = wireMessage{Role: m.Role, Content: m.Content}
	}
	var stop []string
	if len(req.Stop) > 0 {
		stop = append([]string(nil), req.Stop...)
	}
	return wireRequest{
		Model:         model,
		Messages:      msgs,
		Stream:        true,
		StreamOptions: streamOptions{IncludeUsage: true},
		MaxTokens:     req.MaxTokens,
		Temperature:   req.Temperature,
		TopP:          req.TopP,
		Stop:          stop,
		User:          req.User,
	}
}

// Open implements provider.Provider.
func (a *Adapter) Open(ctx context.Context, model string, req *api.ChatRequest) (provider.Stream, error) {
	body, err := json.Marshal(buildRequest(model, req))
	if err != nil {
		return nil, fmt.Errorf("encoding request: %w", err)
	}
	return provider.OpenSSE(ctx, provider.SSEOptions{
		Provider:    a.cfg.Name,
		Client:      a.cfg.Client,
		IdleTimeout: a.cfg.IdleTimeout,
		Translate:   a.translate,
	}, func(ctx context.Context) (*http.Request, error) {
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.BaseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "text/event-stream")
		r.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
		return r, nil
	})
}

type wireChunk struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Role    string `json:"role"`
			Content string `json:"content"`
			// reasoning_content and other vendor extras are intentionally not mapped.
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    any    `json:"code"`
	} `json:"error"`
}

func (a *Adapter) translate(ev sse.Event) (*api.Chunk, bool, error) {
	if strings.TrimSpace(ev.Data) == sse.DoneData {
		return nil, true, nil
	}
	var wc wireChunk
	if err := json.Unmarshal([]byte(ev.Data), &wc); err != nil {
		return nil, false, fmt.Errorf("%s: decoding chunk: %w", a.cfg.Name, err)
	}
	if wc.Error != nil {
		return nil, false, &provider.UpstreamError{
			Provider:  a.cfg.Name,
			Retryable: retryableErrorType(wc.Error.Type),
			Message:   wc.Error.Message,
		}
	}
	out := &api.Chunk{
		ID:      wc.ID,
		Object:  api.ObjectChunk,
		Created: wc.Created,
		Model:   wc.Model,
		Choices: make([]api.Choice, len(wc.Choices)),
	}
	for i, c := range wc.Choices {
		out.Choices[i] = api.Choice{
			Index:        c.Index,
			Delta:        api.Delta{Role: c.Delta.Role, Content: c.Delta.Content},
			FinishReason: c.FinishReason,
		}
	}
	if wc.Usage != nil {
		out.Usage = &api.Usage{
			PromptTokens:     wc.Usage.PromptTokens,
			CompletionTokens: wc.Usage.CompletionTokens,
			TotalTokens:      wc.Usage.TotalTokens,
		}
	}
	return out, false, nil
}

// retryableErrorType classifies in-stream error events by their type string.
func retryableErrorType(t string) bool {
	t = strings.ToLower(t)
	for _, s := range []string{"rate_limit", "overloaded", "server_error", "timeout", "unavailable"} {
		if strings.Contains(t, s) {
			return true
		}
	}
	return false
}
