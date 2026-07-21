package cmd

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func TestPulsarUnmeasurableResetFailsBeforeMutationIntent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/v2/persistent/public/default/orders/partitions":
			http.NotFound(w, r)
		case "/admin/v2/persistent/public/default/orders/stats":
			_, _ = w.Write([]byte(`{"subscriptions":{"billing":{"msgBacklog":4,"consumers":[]}}}`))
		default:
			t.Fatalf("unexpected Pulsar request %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("pulsar-test", mqgovctx.Context{
		Backend:          "pulsar",
		Cluster:          "pulsar-test",
		PulsarServiceURL: "pulsar://127.0.0.1:6650",
		PulsarAdminURL:   server.URL,
		PulsarTenant:     "public",
		PulsarNamespace:  "default",
	}); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	_, err := runCommandForTestAtHome(
		t,
		home,
		"--context", "pulsar-test",
		"--yes",
		"--ticket", "OPS-1",
		"--allow-offset-reset",
		"group", "reset-offset", "billing", "orders",
		"--to", "earliest",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("reset-offset code = %s, want %s; err=%v", got, apperrors.CodeNotImplemented, err)
	}
	auditData, readErr := os.ReadFile(filepath.Join(home, ".mqgov-cli", "audit.log"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if strings.Contains(string(auditData), `"eventType":"mq.offset.reset.intent"`) {
		t.Fatalf("unmeasurable reset persisted a mutation intent: %s", auditData)
	}
}
