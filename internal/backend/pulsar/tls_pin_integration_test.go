//go:build integration

package pulsar

import (
	"context"
	"crypto/tls"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/tlspin"
)

func TestPulsarTLSPinningIntegration(t *testing.T) {
	serviceURL := os.Getenv("PULSAR_TLS_PIN_SERVICE_URL")
	adminURL := os.Getenv("PULSAR_TLS_PIN_ADMIN_URL")
	if strings.TrimSpace(serviceURL) == "" || strings.TrimSpace(adminURL) == "" {
		skipOrFailIntegration(t, "PULSAR_TLS_PIN_SERVICE_URL and PULSAR_TLS_PIN_ADMIN_URL not set")
	}
	pinPath := os.Getenv("PULSAR_TLS_PIN_PATH")
	if pinPath == "" {
		pinPath = filepath.Join(t.TempDir(), "tls_known_hosts")
	}
	backend, err := New(Options{
		ServiceURL: serviceURL,
		AdminURL:   adminURL,
		Tenant:     getenvDefault("PULSAR_TENANT", "public"),
		Namespace:  getenvDefault("PULSAR_NAMESPACE", "default"),
		Cluster:    "tls-pin",
		TLS:        true,
		CACertFile: os.Getenv("PULSAR_TLS_PIN_CA_CERT_FILE"),
		TLSPinPath: pinPath,
		Timeout:    10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer backend.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = dialPulsarTLSService(ctx, serviceURL, backend.tlsConfig)
	if os.Getenv("PULSAR_TLS_PIN_EXPECT_FAILURE") == "true" {
		assertPulsarTLSPinChanged(t, "service", err)
		assertPulsarTLSPinChanged(t, "admin API", backend.Ping(ctx))
		return
	}
	if err != nil {
		t.Fatalf("service TLS dial error = %v", err)
	}
	if err := backend.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	data, err := os.ReadFile(pinPath)
	if err != nil {
		t.Fatalf("ReadFile(pin) error = %v", err)
	}
	serviceEndpoint, err := url.Parse(serviceURL)
	if err != nil {
		t.Fatalf("Parse(service URL) error = %v", err)
	}
	adminEndpoint, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("Parse(admin URL) error = %v", err)
	}
	for _, address := range []string{serviceEndpoint.Host, adminEndpoint.Host} {
		if !strings.Contains(string(data), address+"\ttls-spki-sha256\tSHA256:") {
			t.Fatalf("pin file = %q, want pin for %s", data, address)
		}
	}
}

func dialPulsarTLSService(ctx context.Context, rawURL string, cfg *tls.Config) error {
	endpoint, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 10 * time.Second},
		Config:    cfg,
	}
	conn, err := dialer.DialContext(ctx, "tcp", endpoint.Host)
	if err != nil {
		return err
	}
	return conn.Close()
}

func assertPulsarTLSPinChanged(t *testing.T, endpoint string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s TLS error = nil, want TLS pin rejection", endpoint)
	}
	appErr := tlspin.AppError(err)
	if appErr == nil {
		appErr = apperrors.AsAppError(err)
	}
	if appErr.Code != apperrors.CodeAuthFailed || !strings.Contains(appErr.Message, "TLS certificate pin changed") {
		t.Fatalf("%s TLS error = %v, want TLS pin changed AuthFailed", endpoint, err)
	}
}
