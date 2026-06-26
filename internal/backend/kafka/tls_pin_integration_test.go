//go:build integration

package kafka

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/apperrors"
)

func TestKafkaTLSPinningIntegration(t *testing.T) {
	brokers := os.Getenv("KAFKA_TLS_PIN_BROKERS")
	if strings.TrimSpace(brokers) == "" {
		t.Skip("KAFKA_TLS_PIN_BROKERS not set")
	}
	pinPath := os.Getenv("KAFKA_TLS_PIN_PATH")
	if pinPath == "" {
		pinPath = filepath.Join(t.TempDir(), "tls_known_hosts")
	}
	backend, err := New(Options{
		Brokers:    []string{brokers},
		Cluster:    "tls-pin",
		TLS:        true,
		CACertFile: os.Getenv("KAFKA_TLS_PIN_CA_CERT_FILE"),
		TLSPinPath: pinPath,
		Timeout:    10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer backend.client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = backend.Ping(ctx)
	if os.Getenv("KAFKA_TLS_PIN_EXPECT_FAILURE") == "true" {
		if err == nil {
			t.Fatal("Ping() error = nil, want TLS pin rejection")
		}
		appErr := apperrors.AsAppError(err)
		if appErr.Code != apperrors.CodeAuthFailed || !strings.Contains(appErr.Message, "TLS certificate pin changed") {
			t.Fatalf("Ping() error = %v, want TLS pin changed AuthFailed", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	data, readErr := os.ReadFile(pinPath)
	if readErr != nil {
		t.Fatalf("ReadFile(pin) error = %v", readErr)
	}
	if !strings.Contains(string(data), "\ttls-spki-sha256\tSHA256:") {
		t.Fatalf("pin file = %q, want tls-spki-sha256 pin", data)
	}
}
