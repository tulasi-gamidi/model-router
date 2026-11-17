// Package router picks upstream targets for a request in cost order and falls back to the next
// target on transient failures. It only sees the unified schema and the provider interface.
package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/api"
	"github.com/tulasi-gamidi/model-router/internal/provider"
)

var (
	// ErrUnknownModel means the requested model is neither a route alias nor a known model.
	ErrUnknownModel = errors.New("unknown model")
	// ErrAllTargetsFailed means every target failed with a retryable error.
	ErrAllTargetsFailed = errors.New("all targets failed")
)

// Attempt records one failed try, for error reporting and logs.
type Attempt struct {
	Provider string
	Model    string
	Status   int // upstream HTTP status, 0 for transport/stream errors
	Err      error
}

// RouteError is returned when no stream could be opened. Err is the terminal reason:
// ErrAllTargetsFailed, a non-retryable upstream error, or the client's context error.
type RouteError struct {
	Attempts []Attempt
	Err      error
}

func (e *RouteError) Error() string {
	parts := make([]string, len(e.Attempts))
	for i, a := range e.Attempts {
		parts[i] = a.String()
	}
	return fmt.Sprintf("%v (tried: %s)", e.Err, strings.Join(parts, ", "))
}

func (e *RouteError) Unwrap() error { return e.Err }

func (a Attempt) String() string {
	if a.Status != 0 {
		return fmt.Sprintf("%s/%s: %d", a.Provider, a.Model, a.Status)
	}
	return fmt.Sprintf("%s/%s: %s", a.Provider, a.Model, shortErr(a.Err))
}

func shortErr(err error) string {
	switch {
	case errors.Is(err, provider.ErrIdleTimeout):
		return "idle timeout"
	case errors.Is(err, provider.ErrIncompleteStream):
		return "incomplete stream"
	}
	var ue *provider.UpstreamError
	if errors.As(err, &ue) {
		return "upstream error event"
	}
	return "connection error"
}

// maxPreamble bounds how many role-only chunks are buffered while waiting for the first
// chunk with substance.
const maxPreamble = 8

// Router routes requests to providers.
type Router struct {
	providers    map[string]provider.Provider
	table        map[string][]Target
	defaultRoute string
	log          *slog.Logger
}

// New builds a Router. Targets whose provider is not in providers are dropped (logged);
// a route left with no targets is a startup error.
func New(m Manifest, providers []provider.Provider, log *slog.Logger) (*Router, error) {
	byName := make(map[string]provider.Provider, len(providers))
	configured := make(map[string]bool, len(providers))
	for _, p := range providers {
		byName[p.Name()] = p
		configured[p.Name()] = true
	}
	table, dropped, err := m.compile(configured)
	if err != nil {
		return nil, err
	}
	if len(dropped) > 0 {
		log.Info("manifest targets skipped: provider not configured", "targets", dropped)
	}
	return &Router{providers: byName, table: table, defaultRoute: m.DefaultRoute, log: log}, nil
}

// Resolve returns the cost-ordered targets for a requested model name or alias.
// An empty name resolves to the default route.
func (r *Router) Resolve(name string) ([]Target, error) {
	if name == "" {
		name = r.defaultRoute
	}
	ts, ok := r.table[name]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownModel, name)
	}
	return ts, nil
}

// Route opens a stream on the cheapest target that works. A target counts as working once it
// has produced its first substantive chunk; until then any retryable failure (status, connect,
// header timeout, stall, error event) moves on to the next target. The returned Stream replays
// the buffered chunks. Once Route returns a Stream there is no further fallback.
func (r *Router) Route(ctx context.Context, req *api.ChatRequest) (provider.Stream, Target, error) {
	targets, err := r.Resolve(req.Model)
	if err != nil {
		return nil, Target{}, &RouteError{Err: err}
	}
	var attempts []Attempt
	for i, t := range targets {
		if err := ctx.Err(); err != nil {
			return nil, Target{}, &RouteError{Attempts: attempts, Err: err}
		}
		start := time.Now()
		s, err := r.try(ctx, t, req)
		latency := time.Since(start)
		if err == nil {
			r.log.Info("upstream attempt", "provider", t.Provider, "model", t.UpstreamModel,
				"attempt", i+1, "outcome", "ok", "ttfb_ms", latency.Milliseconds())
			return s, t, nil
		}
		a := Attempt{Provider: t.Provider, Model: t.UpstreamModel, Err: err}
		var ue *provider.UpstreamError
		if errors.As(err, &ue) {
			a.Status = ue.Status
		}
		attempts = append(attempts, a)
		retryable := provider.IsRetryable(ctx, err)
		r.log.Warn("upstream attempt", "provider", t.Provider, "model", t.UpstreamModel,
			"attempt", i+1, "outcome", "failed", "status", a.Status, "retryable", retryable,
			"latency_ms", latency.Milliseconds(), "error", err.Error())
		if !retryable {
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			return nil, Target{}, &RouteError{Attempts: attempts, Err: err}
		}
	}
	return nil, Target{}, &RouteError{Attempts: attempts, Err: ErrAllTargetsFailed}
}

// try opens one target and peeks until the first substantive chunk (or a clean end).
func (r *Router) try(ctx context.Context, t Target, req *api.ChatRequest) (provider.Stream, error) {
	p, ok := r.providers[t.Provider]
	if !ok {
		return nil, fmt.Errorf("provider %q not configured", t.Provider)
	}
	s, err := p.Open(ctx, t.UpstreamModel, req)
	if err != nil {
		return nil, err
	}
	ps := &peekedStream{inner: s}
	for {
		c, err := s.Next()
		if errors.Is(err, io.EOF) {
			ps.endErr = io.EOF // clean, possibly empty, completed stream
			return ps, nil
		}
		if err != nil {
			_ = s.Close()
			return nil, err
		}
		ps.buf = append(ps.buf, c)
		if !isPreamble(c) || len(ps.buf) >= maxPreamble {
			return ps, nil
		}
	}
}

// isPreamble reports whether c only announces the assistant role (no text, finish or usage).
func isPreamble(c *api.Chunk) bool {
	if c.Usage != nil || len(c.Choices) == 0 {
		return false
	}
	for _, ch := range c.Choices {
		if ch.Delta.Content != "" || ch.FinishReason != nil || ch.Delta.Role == "" {
			return false
		}
	}
	return true
}

// peekedStream replays chunks consumed during the peek, then delegates to the inner stream.
type peekedStream struct {
	inner  provider.Stream
	buf    []*api.Chunk
	endErr error // set when the peek already reached the end of the stream
}

func (p *peekedStream) Next() (*api.Chunk, error) {
	if len(p.buf) > 0 {
		c := p.buf[0]
		p.buf = p.buf[1:]
		return c, nil
	}
	if p.endErr != nil {
		return nil, p.endErr
	}
	return p.inner.Next()
}

func (p *peekedStream) Close() error { return p.inner.Close() }
