package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tulasi-gamidi/model-router/internal/api"
)

// Auth rejects requests without a valid gateway key before any upstream work happens.
type Auth struct {
	keyHashes [][32]byte
	log       *slog.Logger
}

// NewAuth returns an Auth accepting any of keys. Keys are stored as SHA-256 digests so the
// comparison is constant-time and length-independent.
func NewAuth(keys []string, log *slog.Logger) *Auth {
	a := &Auth{log: log}
	for _, k := range keys {
		a.keyHashes = append(a.keyHashes, sha256.Sum256([]byte(k)))
	}
	return a
}

// Wrap returns next guarded by the API key check.
func (a *Auth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.valid(r.Header.Get("Authorization")) {
			a.log.Warn("rejected request: invalid gateway key", "path", r.URL.Path, "remote", r.RemoteAddr)
			w.Header().Set("WWW-Authenticate", `Bearer realm="model-router"`)
			writeError(w, http.StatusUnauthorized, api.ErrTypeAuthentication, "invalid or missing API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *Auth) valid(header string) bool {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return false
	}
	got := sha256.Sum256([]byte(strings.TrimSpace(token)))
	match := 0
	for _, want := range a.keyHashes { // no early exit: time does not depend on which key matched
		match |= subtle.ConstantTimeCompare(got[:], want[:])
	}
	return match == 1
}
