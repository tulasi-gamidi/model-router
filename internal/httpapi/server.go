package httpapi

import (
	"net/http"

	"github.com/tulasi-gamidi/model-router/internal/api"
)

// NewMux wires the routes. Auth runs before the chat handler, so a bad key never reaches
// the router or any upstream.
func NewMux(chat http.Handler, auth *Auth) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat/completions", auth.Wrap(chat))
	// Without this, the catch-all "/" below would turn GET /v1/chat/completions into a 404.
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, api.ErrTypeInvalidRequest, "method not allowed; use POST")
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, api.ErrTypeInvalidRequest, "not found")
	})
	return mux
}
