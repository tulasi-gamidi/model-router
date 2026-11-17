// Command server runs the model router gateway. Wiring only: all behaviour lives in internal/.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tulasi-gamidi/model-router/internal/config"
	"github.com/tulasi-gamidi/model-router/internal/httpapi"
	"github.com/tulasi-gamidi/model-router/internal/provider"
	"github.com/tulasi-gamidi/model-router/internal/provider/anthropic"
	"github.com/tulasi-gamidi/model-router/internal/provider/openai"
	"github.com/tulasi-gamidi/model-router/internal/router"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "model-router:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	client := provider.NewHTTPClient(provider.TransportConfig{})
	providers := []provider.Provider{
		openai.New(openai.Config{Name: "digitalocean", BaseURL: cfg.DOBaseURL, APIKey: cfg.DOKey, Client: client, IdleTimeout: cfg.IdleTimeout}),
	}
	if cfg.AnthropicKey != "" {
		providers = append(providers, anthropic.New(anthropic.Config{
			Name: "anthropic", BaseURL: cfg.AnthropicBaseURL, APIKey: cfg.AnthropicKey, Client: client, IdleTimeout: cfg.IdleTimeout,
		}))
	}
	if cfg.EnableDemoFallback {
		// Points at a closed local port: connection refused, so the router must fall back.
		providers = append(providers, openai.New(openai.Config{
			Name: "demo-broken", BaseURL: "http://127.0.0.1:9/v1", APIKey: "none", Client: client, IdleTimeout: cfg.IdleTimeout,
		}))
	}

	rt, err := router.New(cfg.Manifest, providers, log)
	if err != nil {
		return fmt.Errorf("building router: %w", err)
	}

	mux := httpapi.NewMux(httpapi.NewChatHandler(rt, log, httpapi.DefaultMaxBodyBytes), httpapi.NewAuth(cfg.GatewayKeys, log))
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second, // request body only; responses stream
		IdleTimeout:       120 * time.Second,
		// WriteTimeout deliberately unset: it would cut off long streams.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr, "providers", len(providers))
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	log.Info("shutting down: draining in-flight streams")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("graceful shutdown timed out; closing remaining connections", "error", err.Error())
		_ = srv.Close()
	}
	return nil
}
