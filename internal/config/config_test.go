package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/provider"
	"github.com/tulasi-gamidi/model-router/internal/provider/openai"
	"github.com/tulasi-gamidi/model-router/internal/router"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{"DO_MODEL_ACCESS_KEY": "do-key", "GATEWAY_API_KEYS": "key-one-123, key-two-456"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != 8080 || c.IdleTimeout != 30*time.Second || c.LogLevel != slog.LevelInfo || c.EnableDemoFallback {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if len(c.GatewayKeys) != 2 || c.GatewayKeys[1] != "key-two-456" {
		t.Fatalf("keys = %v", c.GatewayKeys)
	}
	if c.Manifest.DefaultRoute != "auto" || len(c.Manifest.Routes) == 0 {
		t.Fatalf("embedded manifest not loaded: %+v", c.Manifest)
	}
}

func TestLoadReportsAllProblems(t *testing.T) {
	_, err := Load(env(map[string]string{"PORT": "abc", "IDLE_TIMEOUT": "-1s", "LOG_LEVEL": "loud", "GATEWAY_API_KEYS": "short"}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DO_MODEL_ACCESS_KEY", "GATEWAY_API_KEYS", "PORT", "IDLE_TIMEOUT", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

func TestParseManifestRejectsUnknownFields(t *testing.T) {
	if _, err := ParseManifest([]byte(`{"default_route":"a","modles":{}}`)); err == nil {
		t.Fatal("typo in manifest should fail")
	}
}

// The embedded manifest must compile against the providers main configures by default
// (digitalocean only, plus the optional ones).
func TestEmbeddedManifestCompiles(t *testing.T) {
	m, err := ParseManifest(defaultManifest)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(name string) provider.Provider { return openai.New(openai.Config{Name: name}) }
	for _, ps := range [][]provider.Provider{
		{mk("digitalocean")},
		{mk("digitalocean"), mk("anthropic"), mk("demo-broken")},
	} {
		r, err := router.New(m, ps, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatalf("providers %d: %v", len(ps), err)
		}
		ts, err := r.Resolve("auto")
		if err != nil || ts[0].Model != "mistral-3-14B" {
			t.Fatalf("auto should start with the cheapest model: %+v %v", ts, err)
		}
	}
}
