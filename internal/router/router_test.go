package router

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/api"
	"github.com/tulasi-gamidi/model-router/internal/provider"
	"github.com/tulasi-gamidi/model-router/internal/provider/openai"
	"github.com/tulasi-gamidi/model-router/internal/testutil"
)

var quiet = slog.New(slog.DiscardHandler)

func fakeProvider(t *testing.T, name, url string) provider.Provider {
	t.Helper()
	return openai.New(openai.Config{
		Name: name, BaseURL: url, APIKey: "k",
		Client:      provider.NewHTTPClient(provider.TransportConfig{ResponseHeaderTimeout: 200 * time.Millisecond}),
		IdleTimeout: 200 * time.Millisecond,
	})
}

func req(model string) *api.ChatRequest {
	return &api.ChatRequest{Model: model, Messages: []api.Message{{Role: "user", Content: "hi"}}}
}

func readText(t *testing.T, s provider.Stream) (string, error) {
	t.Helper()
	var sb strings.Builder
	for {
		c, err := s.Next()
		if err != nil {
			return sb.String(), err
		}
		sb.WriteString(c.Text())
	}
}

// twoTier builds a router where route "r" = cheap model on provider "primary", then pricier
// model on provider "backup".
func twoTier(t *testing.T, primaryURL, backupURL string) *Router {
	t.Helper()
	m := Manifest{
		DefaultRoute: "r",
		Models: map[string][]Target{
			"premium": {{Provider: "backup", UpstreamModel: "up-premium", InputPerMTok: 5, OutputPerMTok: 15}},
			"cheap":   {{Provider: "primary", UpstreamModel: "up-cheap", InputPerMTok: 0.1, OutputPerMTok: 0.3}},
		},
		// Listed premium-first on purpose: ordering must come from cost, not list order.
		Routes: map[string][]string{"r": {"premium", "cheap"}},
	}
	r, err := New(m, []provider.Provider{fakeProvider(t, "primary", primaryURL), fakeProvider(t, "backup", backupURL)}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRouteCheapestFirst(t *testing.T) {
	primary := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{Chunks: []string{"cheap answer"}})
	backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{Chunks: []string{"premium answer"}})
	r := twoTier(t, primary.URL, backup.URL)

	s, target, err := r.Route(t.Context(), req("r"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	text, err := readText(t, s)
	if !errors.Is(err, io.EOF) || text != "cheap answer" {
		t.Fatalf("text=%q err=%v", text, err)
	}
	if target.Provider != "primary" || target.Model != "cheap" {
		t.Fatalf("served by %+v", target)
	}
	if backup.Called() {
		t.Fatal("backup must not be called when the cheapest target works")
	}
}

func TestRouteFallsBackOnTransientFailures(t *testing.T) {
	tests := []struct {
		name string
		mode testutil.Mode
	}{
		{"429", testutil.Mode429},
		{"503", testutil.Mode503},
		{"response header timeout", testutil.ModeSlow},
		{"200 then stall", testutil.ModeStall},
		{"200 then error event", testutil.ModeErrorFirst},
		{"200 then clean end without [DONE]", testutil.ModeNoDone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For nodone, make the primary send nothing but the role chunk, so the failure
			// happens before any substantive chunk.
			opts := testutil.Options{Chunks: []string{"primary"}}
			if tt.mode == testutil.ModeNoDone {
				opts.Chunks = []string{}
			}
			primary := testutil.NewUpstream(t, tt.mode, opts)
			backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{Chunks: []string{"backup ", "answer"}})
			r := twoTier(t, primary.URL, backup.URL)

			s, target, err := r.Route(t.Context(), req("r"))
			if err != nil {
				t.Fatalf("Route: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			text, err := readText(t, s)
			if !errors.Is(err, io.EOF) || text != "backup answer" {
				t.Fatalf("text=%q err=%v", text, err)
			}
			if !primary.Called() || target.Provider != "backup" {
				t.Fatalf("primary called=%v, served by %s", primary.Called(), target.Provider)
			}
		})
	}
}

func TestRouteFallsBackOnConnectionRefused(t *testing.T) {
	backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{Chunks: []string{"backup"}})
	r := twoTier(t, "http://127.0.0.1:1", backup.URL)
	s, target, err := r.Route(t.Context(), req("r"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if text, _ := readText(t, s); text != "backup" || target.Provider != "backup" {
		t.Fatalf("text=%q target=%+v", text, target)
	}
}

func TestRouteNoFallbackOnClientErrors(t *testing.T) {
	for _, mode := range []testutil.Mode{testutil.Mode400, testutil.Mode401} {
		t.Run(string(mode), func(t *testing.T) {
			primary := testutil.NewUpstream(t, mode, testutil.Options{})
			backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
			r := twoTier(t, primary.URL, backup.URL)

			s, _, err := r.Route(t.Context(), req("r"))
			if s != nil {
				t.Fatal("expected no stream")
			}
			var ue *provider.UpstreamError
			if !errors.As(err, &ue) || ue.Retryable {
				t.Fatalf("err = %v, want non-retryable UpstreamError", err)
			}
			if backup.Called() {
				t.Fatal("backup must not be called on a non-retryable error")
			}
		})
	}
}

func TestRouteAllFail(t *testing.T) {
	primary := testutil.NewUpstream(t, testutil.Mode429, testutil.Options{})
	backup := testutil.NewUpstream(t, testutil.Mode503, testutil.Options{})
	r := twoTier(t, primary.URL, backup.URL)

	_, _, err := r.Route(t.Context(), req("r"))
	if !errors.Is(err, ErrAllTargetsFailed) {
		t.Fatalf("err = %v", err)
	}
	var re *RouteError
	if !errors.As(err, &re) || len(re.Attempts) != 2 || re.Attempts[0].Status != 429 || re.Attempts[1].Status != 503 {
		t.Fatalf("attempts = %+v", re)
	}
	if !strings.Contains(err.Error(), "primary/up-cheap: 429") {
		t.Fatalf("error should list attempts: %v", err)
	}
}

func TestRouteClientCancelledStopsImmediately(t *testing.T) {
	t.Run("before routing", func(t *testing.T) {
		primary := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
		backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
		r := twoTier(t, primary.URL, backup.URL)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, err := r.Route(ctx, req("r"))
		if !errors.Is(err, context.Canceled) || primary.Called() || backup.Called() {
			t.Fatalf("err=%v primary=%v backup=%v", err, primary.Called(), backup.Called())
		}
	})
	t.Run("while primary is pending", func(t *testing.T) {
		primary := testutil.NewUpstream(t, testutil.ModeStall, testutil.Options{})
		backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
		r := twoTier(t, primary.URL, backup.URL)
		ctx, cancel := context.WithCancel(t.Context())
		// The primary stalls after 200 (idle timeout 200ms); the client leaves before that.
		time.AfterFunc(50*time.Millisecond, cancel)
		_, _, err := r.Route(ctx, req("r"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if backup.Called() {
			t.Fatal("no fallback once the client is gone")
		}
		select {
		case <-primary.Cancelled():
		case <-time.After(2 * time.Second):
			t.Fatal("primary request was not aborted")
		}
	})
}

func TestRouteConcreteModelNeverSwitchesModel(t *testing.T) {
	a := testutil.NewUpstream(t, testutil.Mode429, testutil.Options{})
	b := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{Chunks: []string{"same model, other provider"}})
	other := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	m := Manifest{
		DefaultRoute: "r",
		Models: map[string][]Target{
			"llama": {
				{Provider: "b", UpstreamModel: "llama-b", InputPerMTok: 2},
				{Provider: "a", UpstreamModel: "llama-a", InputPerMTok: 1},
			},
			"other": {{Provider: "other", UpstreamModel: "o", InputPerMTok: 0.01}},
		},
		Routes: map[string][]string{"r": {"llama", "other"}},
	}
	r, err := New(m, []provider.Provider{fakeProvider(t, "a", a.URL), fakeProvider(t, "b", b.URL), fakeProvider(t, "other", other.URL)}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	s, target, err := r.Route(t.Context(), req("llama"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	text, _ := readText(t, s)
	if text != "same model, other provider" || target.Provider != "b" || target.Model != "llama" {
		t.Fatalf("text=%q target=%+v", text, target)
	}
	if !a.Called() || other.Called() {
		t.Fatalf("a called=%v (want true), other called=%v (want false)", a.Called(), other.Called())
	}
}

func TestRoutePointOfNoReturnIsFirstChunk(t *testing.T) {
	// Primary sends its first content chunk then drops: Route must succeed (no fallback after
	// content was produced) and the failure must surface from Next, after the replayed chunks.
	primary := testutil.NewUpstream(t, testutil.ModeMidFail, testutil.Options{Chunks: []string{"partial"}})
	backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	r := twoTier(t, primary.URL, backup.URL)
	s, target, err := r.Route(t.Context(), req("r"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	text, err := readText(t, s)
	if text != "partial" || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("text=%q err=%v", text, err)
	}
	if target.Provider != "primary" || backup.Called() {
		t.Fatal("no fallback after the first chunk")
	}
}

func TestRouteUnknownAndDefault(t *testing.T) {
	primary := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	backup := testutil.NewUpstream(t, testutil.ModeOK, testutil.Options{})
	r := twoTier(t, primary.URL, backup.URL)

	if _, _, err := r.Route(t.Context(), req("nope")); !errors.Is(err, ErrUnknownModel) {
		t.Fatalf("err = %v", err)
	}
	if primary.Called() || backup.Called() {
		t.Fatal("unknown model must not reach upstream")
	}
	s, target, err := r.Route(t.Context(), req(""))
	if err != nil || target.Provider != "primary" {
		t.Fatalf("default route: %+v %v", target, err)
	}
	_ = s.Close()
}

func TestManifestCompile(t *testing.T) {
	base := func() Manifest {
		return Manifest{
			DefaultRoute: "r",
			Models: map[string][]Target{
				"m1": {{Provider: "p", UpstreamModel: "u1", InputPerMTok: 1}},
				"m2": {{Provider: "missing", UpstreamModel: "u2"}},
			},
			Routes: map[string][]string{"r": {"m1", "m2"}},
		}
	}
	configured := map[string]bool{"p": true}

	table, dropped, err := base().compile(configured)
	if err != nil {
		t.Fatal(err)
	}
	if len(dropped) != 1 || dropped[0] != "m2@missing" || len(table["r"]) != 1 {
		t.Fatalf("dropped=%v route=%v", dropped, table["r"])
	}

	tests := map[string]func(*Manifest){
		"unknown model in route":   func(m *Manifest) { m.Routes["r"] = []string{"zzz"} },
		"default route missing":    func(m *Manifest) { m.DefaultRoute = "nope" },
		"route with no providers":  func(m *Manifest) { m.Routes["only-missing"] = []string{"m2"} },
		"route shadows model name": func(m *Manifest) { m.Routes["m1"] = []string{"m1"} },
		"negative cost":            func(m *Manifest) { m.Models["m1"][0].InputPerMTok = -1 },
		"no routes":                func(m *Manifest) { m.Routes = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			m := base()
			mutate(&m)
			if _, _, err := m.compile(configured); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
