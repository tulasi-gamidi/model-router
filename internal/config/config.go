// Package config loads and validates gateway configuration from environment variables and
// the routes/cost manifest.
package config

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/router"
)

//go:embed routes.json
var defaultManifest []byte

// Config is the validated runtime configuration.
type Config struct {
	Port               int
	GatewayKeys        []string
	DOKey              string
	DOBaseURL          string
	AnthropicKey       string // optional
	AnthropicBaseURL   string
	IdleTimeout        time.Duration
	LogLevel           slog.Level
	EnableDemoFallback bool
	Manifest           router.Manifest
}

// Load reads configuration using getenv (os.Getenv in production) and validates it.
// All problems are reported together so startup fails once with a clear message.
func Load(getenv func(string) string) (Config, error) {
	var errs []error
	c := Config{
		DOKey:            strings.TrimSpace(getenv("DO_MODEL_ACCESS_KEY")),
		DOBaseURL:        withDefault(getenv("DO_BASE_URL"), "https://inference.do-ai.run/v1"),
		AnthropicKey:     strings.TrimSpace(getenv("ANTHROPIC_API_KEY")),
		AnthropicBaseURL: withDefault(getenv("ANTHROPIC_BASE_URL"), "https://api.anthropic.com/v1"),
	}
	if c.DOKey == "" {
		errs = append(errs, errors.New("DO_MODEL_ACCESS_KEY is required"))
	}
	for _, k := range strings.Split(getenv("GATEWAY_API_KEYS"), ",") {
		if k = strings.TrimSpace(k); k != "" {
			if len(k) < 8 {
				errs = append(errs, errors.New("GATEWAY_API_KEYS entries must be at least 8 characters"))
				continue
			}
			c.GatewayKeys = append(c.GatewayKeys, k)
		}
	}
	if len(c.GatewayKeys) == 0 {
		errs = append(errs, errors.New("GATEWAY_API_KEYS is required (comma-separated)"))
	}

	port, err := strconv.Atoi(withDefault(getenv("PORT"), "8080"))
	if err != nil || port <= 0 || port > 65535 {
		errs = append(errs, fmt.Errorf("PORT %q is not a valid port", getenv("PORT")))
	}
	c.Port = port

	c.IdleTimeout, err = time.ParseDuration(withDefault(getenv("IDLE_TIMEOUT"), "30s"))
	if err != nil || c.IdleTimeout <= 0 {
		errs = append(errs, fmt.Errorf("IDLE_TIMEOUT %q is not a positive duration", getenv("IDLE_TIMEOUT")))
	}

	if err := c.LogLevel.UnmarshalText([]byte(withDefault(getenv("LOG_LEVEL"), "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}

	c.EnableDemoFallback, err = strconv.ParseBool(withDefault(getenv("ENABLE_DEMO_FALLBACK"), "false"))
	if err != nil {
		errs = append(errs, fmt.Errorf("ENABLE_DEMO_FALLBACK %q is not a boolean", getenv("ENABLE_DEMO_FALLBACK")))
	}

	raw := defaultManifest
	if path := getenv("ROUTES_FILE"); path != "" {
		if raw, err = os.ReadFile(path); err != nil {
			errs = append(errs, fmt.Errorf("reading ROUTES_FILE: %w", err))
		}
	}
	if raw != nil {
		if c.Manifest, err = ParseManifest(raw); err != nil {
			errs = append(errs, err)
		}
	}
	return c, errors.Join(errs...)
}

// ParseManifest decodes a routes/cost manifest, rejecting unknown fields.
func ParseManifest(raw []byte) (router.Manifest, error) {
	var m router.Manifest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return router.Manifest{}, fmt.Errorf("parsing routes manifest: %w", err)
	}
	return m, nil
}

func withDefault(v, def string) string {
	if v = strings.TrimSpace(v); v == "" {
		return def
	}
	return v
}
