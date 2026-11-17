package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/api"
	"github.com/tulasi-gamidi/model-router/internal/sse"
)

// ErrorBodyLimit caps how much of a non-200 upstream body is read.
const ErrorBodyLimit = 4 << 10

// DefaultIdleTimeout is the maximum silence between upstream events.
const DefaultIdleTimeout = 30 * time.Second

// TranslateFunc converts one vendor SSE event into a unified chunk.
// Return (nil, false, nil) to skip an event (pings, block boundaries),
// (nil, true, nil) when the event is the vendor's terminator,
// or an error (typically *UpstreamError) for an in-stream vendor error.
type TranslateFunc func(ev sse.Event) (chunk *api.Chunk, done bool, err error)

// SSEOptions configure OpenSSE.
type SSEOptions struct {
	Provider    string
	Client      *http.Client
	IdleTimeout time.Duration
	// RetryableStatus classifies non-200 statuses; defaults to DefaultRetryableStatus.
	RetryableStatus func(status int) bool
	// Translate is required.
	Translate TranslateFunc
}

// OpenSSE is the shared plumbing behind every adapter's Open. It sends the request built by
// build (which must use the ctx it is given), checks the status before returning, and wraps
// the body in a Stream that enforces the idle timeout and terminator rules.
func OpenSSE(ctx context.Context, opts SSEOptions, build func(ctx context.Context) (*http.Request, error)) (Stream, error) {
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = DefaultIdleTimeout
	}
	if opts.RetryableStatus == nil {
		opts.RetryableStatus = DefaultRetryableStatus
	}
	ctx, cancel := context.WithCancelCause(ctx)

	req, err := build(ctx)
	if err != nil {
		cancel(nil)
		return nil, fmt.Errorf("%s: building request: %w", opts.Provider, err)
	}
	resp, err := opts.Client.Do(req)
	if err != nil {
		cancel(nil)
		return nil, fmt.Errorf("%s: sending request: %w", opts.Provider, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		defer cancel(nil)
		body, _ := io.ReadAll(io.LimitReader(resp.Body, ErrorBodyLimit))
		return nil, &UpstreamError{
			Provider:   opts.Provider,
			Status:     resp.StatusCode,
			Retryable:  opts.RetryableStatus(resp.StatusCode),
			Message:    ExtractErrorMessage(body),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	}

	s := &sseStream{
		provider:  opts.Provider,
		body:      resp.Body,
		rd:        sse.NewReader(resp.Body),
		translate: opts.Translate,
		idle:      opts.IdleTimeout,
		cancel:    cancel,
		cause:     func() error { return context.Cause(ctx) },
	}
	// The idle timer only runs while Next is waiting on the upstream (see Next), so a slow
	// client applying backpressure is never mistaken for a stalled provider.
	s.timer = time.AfterFunc(opts.IdleTimeout, func() { cancel(ErrIdleTimeout) })
	s.timer.Stop()
	return s, nil
}

type sseStream struct {
	provider  string
	body      io.ReadCloser
	rd        *sse.Reader
	translate TranslateFunc
	idle      time.Duration
	timer     *time.Timer
	cancel    context.CancelCauseFunc
	cause     func() error // reports why the stream's context ended, if it did

	err       error // sticky terminal error (io.EOF after terminator)
	closeOnce sync.Once
}

func (s *sseStream) Next() (*api.Chunk, error) {
	if s.err != nil {
		return nil, s.err
	}
	defer s.timer.Stop()
	for {
		s.timer.Reset(s.idle) // armed only while waiting for the next upstream event
		ev, err := s.rd.Next()
		if err != nil {
			s.err = s.readError(err)
			return nil, s.err
		}
		chunk, done, err := s.translate(ev)
		switch {
		case err != nil:
			s.err = err
			return nil, err
		case done:
			s.err = io.EOF
			return nil, io.EOF
		case chunk == nil:
			continue
		default:
			return chunk, nil
		}
	}
}

// readError turns a body read failure into the error reported to callers. A cancelled
// context explains the failure better than the transport error it caused.
func (s *sseStream) readError(err error) error {
	if c := s.cause(); c != nil {
		return fmt.Errorf("%s: reading stream: %w", s.provider, c)
	}
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%s: %w", s.provider, ErrIncompleteStream)
	}
	return fmt.Errorf("%s: reading stream: %w", s.provider, err)
}

func (s *sseStream) Close() error {
	s.closeOnce.Do(func() {
		s.timer.Stop()
		s.cancel(nil)
		_ = s.body.Close()
	})
	return nil
}

// ExtractErrorMessage pulls a human-readable message out of a vendor error body, falling back
// to the trimmed raw body. Handles {"error":{"message":..}}, {"error":"..."} and {"message":..}.
func ExtractErrorMessage(body []byte) string {
	var v struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	if json.Unmarshal(body, &v) == nil {
		var inner struct {
			Message string `json:"message"`
		}
		if len(v.Error) > 0 && json.Unmarshal(v.Error, &inner) == nil && inner.Message != "" {
			return inner.Message
		}
		var s string
		if len(v.Error) > 0 && json.Unmarshal(v.Error, &s) == nil && s != "" {
			return s
		}
		if v.Message != "" {
			return v.Message
		}
	}
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		return "no error body"
	}
	return msg
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
