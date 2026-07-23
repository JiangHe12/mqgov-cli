//go:build integration

package rabbitmq

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestRabbitMQTLSPinningIntegration(t *testing.T) {
	amqpURL := os.Getenv("RABBITMQ_TLS_PIN_AMQP_URL")
	managementURL := os.Getenv("RABBITMQ_TLS_PIN_MANAGEMENT_URL")
	if strings.TrimSpace(amqpURL) == "" || strings.TrimSpace(managementURL) == "" {
		skipOrFailIntegration(t, "RABBITMQ_TLS_PIN_AMQP_URL and RABBITMQ_TLS_PIN_MANAGEMENT_URL not set")
	}
	pinPath := os.Getenv("RABBITMQ_TLS_PIN_PATH")
	if pinPath == "" {
		pinPath = filepath.Join(t.TempDir(), "tls_known_hosts")
	}
	backend, err := New(Options{
		AMQPURL:       amqpURL,
		ManagementURL: managementURL,
		Cluster:       "tls-pin",
		Username:      getenvDefault("RABBITMQ_TLS_PIN_USERNAME", "guest"),
		Password:      getenvDefault("RABBITMQ_TLS_PIN_PASSWORD", "guest"),
		TLS:           true,
		CACertFile:    os.Getenv("RABBITMQ_TLS_PIN_CA_CERT_FILE"),
		TLSPinPath:    pinPath,
		Timeout:       10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer backend.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = backend.Ping(ctx)
	if os.Getenv("RABBITMQ_TLS_PIN_EXPECT_FAILURE") == "true" {
		assertRabbitMQTLSPinChanged(t, "AMQP", err)
		_, managementErr := backend.ListTopics(ctx, mqgov.TopicListOptions{Limit: 1})
		assertRabbitMQTLSPinChanged(t, "management API", managementErr)
		return
	}
	if err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if _, err := backend.ListTopics(ctx, mqgov.TopicListOptions{Limit: 1}); err != nil {
		t.Fatalf("ListTopics() error = %v", err)
	}
	data, err := os.ReadFile(pinPath)
	if err != nil {
		t.Fatalf("ReadFile(pin) error = %v", err)
	}
	amqpEndpoint, err := url.Parse(amqpURL)
	if err != nil {
		t.Fatalf("Parse(AMQP URL) error = %v", err)
	}
	managementEndpoint, err := url.Parse(managementURL)
	if err != nil {
		t.Fatalf("Parse(management URL) error = %v", err)
	}
	for _, address := range []string{amqpEndpoint.Host, managementEndpoint.Host} {
		if !strings.Contains(string(data), address+"\ttls-spki-sha256\tSHA256:") {
			t.Fatalf("pin file = %q, want pin for %s", data, address)
		}
	}
}

func assertRabbitMQTLSPinChanged(t *testing.T, endpoint string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s error = nil, want TLS pin rejection", endpoint)
	}
	appErr := apperrors.AsAppError(err)
	if appErr.Code != apperrors.CodeAuthFailed || !strings.Contains(appErr.Message, "TLS certificate pin changed") {
		t.Fatalf("%s error = %v, want TLS pin changed AuthFailed", endpoint, err)
	}
}
