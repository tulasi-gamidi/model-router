// Package provider defines the adapter boundary between the router and upstream LLM vendors,
// plus the shared streaming plumbing that every adapter uses.
package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/api"
)

// Provider is one upstream vendor endpoint. Adapters implement it; the Router calls it.
type Provider interface {
	// Name identifies the provider in logs and the X-Router-Provider header.
	Name() string
	// Open sends req upstream for the concrete model with stream=true and returns once the
	// HTTP status line has been read. A non-200 status closes the body and returns *UpstreamError.
	//
	// Open MUST NOT mutate req: the same pointer is reused for every fallback attempt.
	// Adapters build a fresh vendor request from it instead.
	//
	// ctx governs the whole stream: cancelling it aborts the upstream request and unblocks Next.
	Open(ctx context.Context, model string, req *api.ChatRequest) (Stream, error)
}

// Stream yields unified chunks translated from the vendor stream. It is pull-based and not
// safe for concurrent use.
//
// Next takes no context on purpose: the stream is bound to the ctx given to Open (like an
// http.Response body), and that ctx is what the idle timer and client disconnect cancel.
type Stream interface {
	// Next returns the next chunk. io.EOF means the upstream sent its terminator ([DONE] /
	// message_stop). Any other error is a failure: dropped connection, idle timeout,
	// ErrIncompleteStream or an in-stream vendor error (*UpstreamError).
	Next() (*api.Chunk, error)
	// Close aborts the upstream request and releases the connection. Idempotent and safe to
	// call after any error.
	Close() error
}

var (
	// ErrIncompleteStream means the upstream body ended cleanly but without its terminator.
	ErrIncompleteStream = errors.New("upstream stream ended without terminator")
	// ErrIdleTimeout means no upstream event arrived within the idle timeout.
	ErrIdleTimeout = errors.New("upstream idle timeout")
)

// UpstreamError is a failure reported by the upstream itself: a non-200 status, or an error
// event inside a 200 stream (Status 0).
type UpstreamError struct {
	Provider   string
	Status     int
	Retryable  bool          // decided by the adapter, which knows its vendor's codes
	Message    string        // at most ErrorBodyLimit bytes of upstream detail; never logged at info with content
	RetryAfter time.Duration // from Retry-After, 0 if absent
}

func (e *UpstreamError) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("%s: in-stream error: %s", e.Provider, e.Message)
	}
	return fmt.Sprintf("%s: upstream status %d: %s", e.Provider, e.Status, e.Message)
}

// DefaultRetryableStatus reports whether status is a transient failure that warrants trying
// another target. Adapters may extend it (e.g. Anthropic's 529 Overloaded).
func DefaultRetryableStatus(status int) bool {
	switch status {
	case 408, 429, 502, 503, 504:
		return true
	}
	return false
}

// IsRetryable is the single fallback classifier used by the Router. ctx is the client's
// request context: once it is done the client is gone and nothing is retried, which is
// checked first because a cancelled request often surfaces as a wrapped network error.
func IsRetryable(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	var ue *UpstreamError
	if errors.As(err, &ue) {
		return ue.Retryable
	}
	if errors.Is(err, ErrIdleTimeout) || errors.Is(err, ErrIncompleteStream) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// Connection closed before/while reading the response.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	// Dial/TLS/response-header timeouts and other transport failures.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}
