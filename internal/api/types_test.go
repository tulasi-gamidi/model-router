package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	f := false
	tr := true
	zero := 0
	hot := 3.0
	tests := []struct {
		name    string
		req     ChatRequest
		wantErr string
	}{
		{"ok minimal", ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}}, ""},
		{"ok stream true", ChatRequest{Stream: &tr, Messages: []Message{{Role: "user", Content: "hi"}}}, ""},
		{"ok empty assistant", ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}, {Role: "assistant"}}}, ""},
		{"stream false rejected", ChatRequest{Stream: &f, Messages: []Message{{Role: "user", Content: "hi"}}}, "stream=true"},
		{"no messages", ChatRequest{}, "at least one"},
		{"bad role", ChatRequest{Messages: []Message{{Role: "tool", Content: "x"}}}, "role"},
		{"empty user content", ChatRequest{Messages: []Message{{Role: "user"}}}, "content"},
		{"max_tokens zero", ChatRequest{MaxTokens: &zero, Messages: []Message{{Role: "user", Content: "hi"}}}, "max_tokens"},
		{"temperature out of range", ChatRequest{Temperature: &hot, Messages: []Message{{Role: "user", Content: "hi"}}}, "temperature"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestStopListUnmarshal(t *testing.T) {
	tests := []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{`{"stop":"x"}`, []string{"x"}, false},
		{`{"stop":["a","b"]}`, []string{"a", "b"}, false},
		{`{"stop":5}`, nil, true},
	}
	for _, tt := range tests {
		var r ChatRequest
		err := json.Unmarshal([]byte(tt.in), &r)
		if tt.wantErr {
			if err == nil {
				t.Errorf("%s: expected error", tt.in)
			}
			continue
		}
		if err != nil || strings.Join(r.Stop, ",") != strings.Join(tt.want, ",") {
			t.Errorf("%s: got %v, %v", tt.in, r.Stop, err)
		}
	}
}

func TestChunkJSONShape(t *testing.T) {
	c := Chunk{ID: "x", Object: ObjectChunk, Model: "m", Choices: []Choice{{Delta: Delta{Content: "hi"}}}}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	// finish_reason must be present as null (OpenAI clients expect the key); usage omitted when nil.
	if !strings.Contains(s, `"finish_reason":null`) || strings.Contains(s, "usage") {
		t.Fatalf("unexpected shape: %s", s)
	}
}
