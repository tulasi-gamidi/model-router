package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/api"
	"github.com/tulasi-gamidi/model-router/internal/provider"
	"github.com/tulasi-gamidi/model-router/internal/router"
	"github.com/tulasi-gamidi/model-router/internal/sse"
)

// Router is what the handler needs from the router.
type Router interface {
	Route(ctx context.Context, req *api.ChatRequest) (provider.Stream, router.Target, error)
}

// DefaultMaxBodyBytes caps request bodies.
const DefaultMaxBodyBytes = 1 << 20

// ChatHandler serves POST /v1/chat/completions as an SSE stream.
type ChatHandler struct {
	router       Router
	log          *slog.Logger
	maxBodyBytes int64
}

// NewChatHandler returns a ChatHandler. maxBodyBytes <= 0 uses DefaultMaxBodyBytes.
func NewChatHandler(r Router, log *slog.Logger, maxBodyBytes int64) *ChatHandler {
	if maxBodyBytes <= 0 {
		maxBodyBytes = DefaultMaxBodyBytes
	}
	return &ChatHandler{router: r, log: log, maxBodyBytes: maxBodyBytes}
}

// mid-stream error event payload; the stream then ends without [DONE].
var streamInterrupted, _ = json.Marshal(api.ErrorResponse{Error: api.ErrorBody{
	Message: "upstream stream interrupted", Type: api.ErrTypeUpstream,
}})

func (h *ChatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	reqID := newRequestID()
	w.Header().Set("X-Request-Id", reqID)
	log := h.log.With("request_id", reqID)

	var req api.ChatRequest
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, api.ErrTypeInvalidRequest,
				fmt.Sprintf("request body exceeds %d bytes", h.maxBodyBytes))
			return
		}
		writeError(w, http.StatusBadRequest, api.ErrTypeInvalidRequest, "invalid JSON body: "+err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, api.ErrTypeInvalidRequest, err.Error())
		return
	}

	// Fallback zone: nothing has been written to the client yet.
	stream, target, err := h.router.Route(ctx, &req)
	if err != nil {
		if ctx.Err() != nil {
			log.Info("client disconnected before response", "route", req.Model, "duration_ms", time.Since(start).Milliseconds())
			return
		}
		status, typ, msg := routeErrorResponse(err)
		log.Warn("request failed before streaming", "route", req.Model, "status", status, "error", err.Error())
		writeError(w, status, typ, msg)
		return
	}
	defer stream.Close()

	// Point of no return: commit to 200 + SSE.
	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("Connection", "keep-alive")
	hdr.Set("X-Accel-Buffering", "no")
	hdr.Set("X-Router-Provider", target.Provider)
	hdr.Set("X-Router-Model", target.UpstreamModel)
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)

	var (
		chunks  int
		usage   *api.Usage
		outcome = "completed"
	)
	defer func() {
		attrs := []any{"route", req.Model, "provider", target.Provider, "model", target.UpstreamModel,
			"outcome", outcome, "chunks", chunks, "duration_ms", time.Since(start).Milliseconds()}
		if usage != nil {
			attrs = append(attrs, "prompt_tokens", usage.PromptTokens, "completion_tokens", usage.CompletionTokens,
				"est_cost_usd", target.EstimateUSD(usage.PromptTokens, usage.CompletionTokens))
		}
		log.Info("chat request", attrs...)
	}()

	for {
		chunk, err := stream.Next()
		if errors.Is(err, io.EOF) {
			if sse.WriteDone(w) != nil || rc.Flush() != nil {
				outcome = "client_disconnected"
			}
			return
		}
		if err != nil {
			if ctx.Err() != nil {
				outcome = "client_disconnected"
				return
			}
			outcome = "upstream_failed_midstream"
			log.Warn("upstream failed mid-stream", "provider", target.Provider, "model", target.UpstreamModel, "error", err.Error())
			_ = sse.WriteData(w, streamInterrupted)
			_ = rc.Flush()
			return // deliberately no [DONE]
		}
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
		b, err := json.Marshal(chunk)
		if err != nil {
			outcome = "encode_error"
			_ = sse.WriteData(w, streamInterrupted)
			_ = rc.Flush()
			return
		}
		if sse.WriteData(w, b) != nil || rc.Flush() != nil {
			outcome = "client_disconnected" // write failed: client is gone; deferred Close aborts upstream
			return
		}
		chunks++
	}
}

// routeErrorResponse maps a routing failure (before any byte was streamed) to an HTTP response.
func routeErrorResponse(err error) (status int, typ, msg string) {
	if errors.Is(err, router.ErrUnknownModel) {
		return http.StatusBadRequest, api.ErrTypeInvalidRequest, err.Error()
	}
	if errors.Is(err, router.ErrAllTargetsFailed) {
		return http.StatusBadGateway, api.ErrTypeNoAvailableProvider, "all upstream providers failed: " + err.Error()
	}
	var ue *provider.UpstreamError
	if errors.As(err, &ue) {
		switch ue.Status {
		case http.StatusBadRequest, http.StatusUnprocessableEntity, http.StatusRequestEntityTooLarge:
			return http.StatusBadRequest, api.ErrTypeInvalidRequest, "upstream rejected the request: " + ue.Message
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
			// The client's key was fine; the gateway's provider config is not. Don't leak details.
			return http.StatusBadGateway, api.ErrTypeUpstreamConfig,
				fmt.Sprintf("provider %s refused the gateway's credentials or model access (status %d)", ue.Provider, ue.Status)
		}
	}
	return http.StatusBadGateway, api.ErrTypeUpstream, "upstream request failed"
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
