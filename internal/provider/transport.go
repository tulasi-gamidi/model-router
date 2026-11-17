package provider

import (
	"net"
	"net/http"
	"time"
)

// TransportConfig holds the connection-level timeouts. Zero values take the defaults.
type TransportConfig struct {
	ConnectTimeout        time.Duration // TCP connect, default 5s
	TLSHandshakeTimeout   time.Duration // default 5s
	ResponseHeaderTimeout time.Duration // time to upstream status line, default 30s
}

// NewHTTPClient returns a client tuned for long-lived streaming. It deliberately sets no
// http.Client.Timeout: that would cover reading the body and cut off long generations.
// Stalls mid-body are handled by the per-stream idle timer instead.
func NewHTTPClient(cfg TransportConfig) *http.Client {
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.TLSHandshakeTimeout == 0 {
		cfg.TLSHandshakeTimeout = 5 * time.Second
	}
	if cfg.ResponseHeaderTimeout == 0 {
		cfg.ResponseHeaderTimeout = 30 * time.Second
	}
	dialer := &net.Dialer{Timeout: cfg.ConnectTimeout, KeepAlive: 30 * time.Second}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
			ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   20,
			IdleConnTimeout:       90 * time.Second,
		},
	}
}
