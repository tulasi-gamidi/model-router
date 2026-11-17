// Package api defines the unified, vendor-neutral schema of the gateway: the OpenAI chat
// completions format, text content only. Handler and Router only ever see these types.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ChatRequest is the unified inbound request body.
type ChatRequest struct {
	Model       string    `json:"model,omitempty"`
	Messages    []Message `json:"messages"`
	Stream      *bool     `json:"stream,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
	Temperature *float64  `json:"temperature,omitempty"`
	TopP        *float64  `json:"top_p,omitempty"`
	Stop        StopList  `json:"stop,omitempty"`
	User        string    `json:"user,omitempty"`
}

// Message is one chat turn. Content is text only (multimodal is out of scope).
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// StopList accepts the OpenAI "stop" field as either a string or an array of strings.
type StopList []string

// UnmarshalJSON implements json.Unmarshaler.
func (s *StopList) UnmarshalJSON(b []byte) error {
	var one string
	if err := json.Unmarshal(b, &one); err == nil {
		*s = StopList{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return errors.New("stop must be a string or an array of strings")
	}
	*s = many
	return nil
}

// Roles accepted in Message.Role.
const (
	RoleSystem    = "system"
	RoleDeveloper = "developer"
	RoleUser      = "user"
	RoleAssistant = "assistant"
)

// Validate checks the request without modifying it.
func (r *ChatRequest) Validate() error {
	if r.Stream != nil && !*r.Stream {
		return errors.New("only stream=true is supported by this gateway")
	}
	if len(r.Messages) == 0 {
		return errors.New("messages must contain at least one message")
	}
	for i, m := range r.Messages {
		switch m.Role {
		case RoleSystem, RoleDeveloper, RoleUser, RoleAssistant:
		default:
			return fmt.Errorf("messages[%d].role %q is not supported", i, m.Role)
		}
		if m.Content == "" && m.Role != RoleAssistant {
			return fmt.Errorf("messages[%d].content must not be empty", i)
		}
	}
	if r.MaxTokens != nil && *r.MaxTokens <= 0 {
		return errors.New("max_tokens must be positive")
	}
	if r.Temperature != nil && (*r.Temperature < 0 || *r.Temperature > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	if r.TopP != nil && (*r.TopP < 0 || *r.TopP > 1) {
		return errors.New("top_p must be between 0 and 1")
	}
	return nil
}

// Chunk is one unified streaming event: an OpenAI chat.completion.chunk.
type Chunk struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// ObjectChunk is the value of Chunk.Object.
const ObjectChunk = "chat.completion.chunk"

// Choice is one streamed choice delta.
type Choice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// Delta is the incremental message content.
type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// Usage reports token counts, usually on the final chunk.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Text returns the concatenated delta content of all choices in the chunk.
func (c *Chunk) Text() string {
	if len(c.Choices) == 1 {
		return c.Choices[0].Delta.Content
	}
	var s string
	for _, ch := range c.Choices {
		s += ch.Delta.Content
	}
	return s
}

// Finish reasons used by adapters.
const (
	FinishStop   = "stop"
	FinishLength = "length"
)

// StrPtr returns a pointer to s; handy for FinishReason.
func StrPtr(s string) *string { return &s }

// ErrorResponse is the OpenAI-shaped error body.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody is the inner error object.
type ErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// Error types returned to clients.
const (
	ErrTypeInvalidRequest      = "invalid_request_error"
	ErrTypeAuthentication      = "authentication_error"
	ErrTypeUpstream            = "upstream_error"
	ErrTypeUpstreamConfig      = "upstream_config_error"
	ErrTypeNoAvailableProvider = "no_available_provider"
)
