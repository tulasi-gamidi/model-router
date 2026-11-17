package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"syscall"
	"testing"
	"time"
)

func TestIsRetryable(t *testing.T) {
	live := context.Background()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	connRefused := &url.Error{Op: "Post", URL: "http://x", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"nil", live, nil, false},
		{"429 marked retryable", live, &UpstreamError{Status: 429, Retryable: true}, true},
		{"529 marked retryable by adapter", live, fmt.Errorf("wrap: %w", &UpstreamError{Status: 529, Retryable: true}), true},
		{"400 not retryable", live, &UpstreamError{Status: 400}, false},
		{"401 not retryable", live, &UpstreamError{Status: 401}, false},
		{"idle timeout", live, fmt.Errorf("x: %w", ErrIdleTimeout), true},
		{"incomplete stream", live, fmt.Errorf("x: %w", ErrIncompleteStream), true},
		{"connection refused", live, connRefused, true},
		{"connection reset", live, fmt.Errorf("read: %w", syscall.ECONNRESET), true},
		{"server closed before response", live, &url.Error{Op: "Post", URL: "http://x", Err: io.EOF}, true},
		{"context canceled", live, &url.Error{Op: "Post", URL: "http://x", Err: context.Canceled}, false},
		{"client gone even if error looks transient", cancelled, connRefused, false},
		{"client gone with 429", cancelled, &UpstreamError{Status: 429, Retryable: true}, false},
		{"unknown error", live, errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetryable(tt.ctx, tt.err); got != tt.want {
				t.Fatalf("IsRetryable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestExtractErrorMessage(t *testing.T) {
	tests := map[string]string{
		`{"error":{"message":"m1","type":"x"}}`:                "m1",
		`{"type":"error","error":{"type":"o","message":"m2"}}`: "m2",
		`{"error":"m3"}`:   "m3",
		`{"message":"m4"}`: "m4",
		"plain text\n":     "plain text",
		"":                 "no error body",
	}
	for in, want := range tests {
		if got := ExtractErrorMessage([]byte(in)); got != want {
			t.Errorf("ExtractErrorMessage(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("7"); got != 7*time.Second {
		t.Fatalf("got %v", got)
	}
	if got := parseRetryAfter("nonsense"); got != 0 {
		t.Fatalf("got %v", got)
	}
}
