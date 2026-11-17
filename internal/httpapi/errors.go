// Package httpapi is the HTTP surface: auth middleware, the streaming chat handler, health
// check and OpenAI-shaped JSON errors.
package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/tulasi-gamidi/model-router/internal/api"
)

// writeError writes an OpenAI-shaped JSON error. Only valid before the stream has started.
func writeError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(api.ErrorResponse{Error: api.ErrorBody{Message: msg, Type: typ}})
}
