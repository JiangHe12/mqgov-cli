package cmd

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/credstore"
	corectx "github.com/JiangHe12/opskit-core/ctx"
	"github.com/JiangHe12/opskit-core/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func TestCtxSetRejectsPlainCredential(t *testing.T) {
	mqgovctx.SetConfigPath(filepath.Join(t.TempDir(), "config.yaml"))
	_, err := runCommandForTest(t,
		"-o", "json",
		"--backend", "rocketmq",
		"ctx", "set", "dev",
		"--nameservers", "127.0.0.1:9876",
		"--access-key", "ak",
		"--password", "secret-key",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeUsageError {
		t.Fatalf("error code = %s, want %s; err=%v", got, apperrors.CodeUsageError, err)
	}
}

func TestCtxSetStoresCredentialReference(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Setenv("MQGOV_CLI_CREDENTIAL_PASSPHRASE", "test-passphrase")

	out, err := runCommandForTest(t,
		"-o", "json",
		"--backend", "rocketmq",
		"ctx", "set", "dev",
		"--nameservers", "127.0.0.1:9876",
		"--broker-addr", "127.0.0.1:10911",
		"--access-key", "ak",
		"--password", "secret-key",
		"--credential-backend", "encrypted-file",
	)
	if err != nil {
		t.Fatalf("ctx set error = %v; out=%s", err, out)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-key") {
		t.Fatalf("context file leaked credential: %s", data)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	ref := credstore.ParseRef(cfg.Contexts["dev"].Password)
	if !ref.IsRef || ref.BackendName != "encrypted-file" {
		t.Fatalf("password ref = %#v; raw=%q", ref, cfg.Contexts["dev"].Password)
	}
}

func TestCtxSetStoresKafkaSchemaRegistryCredentialReference(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Setenv("MQGOV_CLI_CREDENTIAL_PASSPHRASE", "test-passphrase")

	out, err := runCommandForTest(t,
		"-o", "json",
		"--backend", "kafka",
		"ctx", "set", "dev",
		"--brokers", "127.0.0.1:9092",
		"--schema-registry-url", "https://schema-registry.example",
		"--schema-registry-username", "sr-user",
		"--schema-registry-password", "sr-secret",
		"--credential-backend", "encrypted-file",
	)
	if err != nil {
		t.Fatalf("ctx set error = %v; out=%s", err, out)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "sr-secret") {
		t.Fatalf("context file leaked schema registry credential: %s", data)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	item := cfg.Contexts["dev"]
	if item.Password != "" {
		t.Fatalf("broker password ref = %q, want empty when only schema registry password was provided", item.Password)
	}
	if item.KafkaSchemaRegistryURL != "https://schema-registry.example" || item.KafkaSchemaRegistryUsername != "sr-user" {
		t.Fatalf("schema registry context fields = %+v", item)
	}
	ref := credstore.ParseRef(item.KafkaSchemaRegistryPassword)
	if !ref.IsRef || ref.BackendName != "encrypted-file" {
		t.Fatalf("schema registry password ref = %#v; raw=%q", ref, item.KafkaSchemaRegistryPassword)
	}
	resolved, err := mqgovctx.ResolveKafkaSchemaRegistryPassword(t.Context(), "dev", item)
	if err != nil {
		t.Fatalf("ResolveKafkaSchemaRegistryPassword() error = %v", err)
	}
	if resolved != "sr-secret" {
		t.Fatalf("resolved schema registry password = %q, want sr-secret", resolved)
	}
}

func TestCtxSetStoresRabbitMQUsername(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })

	out, err := runCommandForTest(t,
		"-o", "json",
		"--config", configPath,
		"--backend", "rabbitmq",
		"ctx", "set", "dev",
		"--host", "127.0.0.1",
		"--port", "5672",
		"--vhost", "/",
		"--management-url", "http://127.0.0.1:15672",
		"--username", "guest",
	)
	if err != nil {
		t.Fatalf("ctx set rabbitmq error = %v; out=%s", err, out)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	item := cfg.Contexts["dev"]
	if item.Username != "guest" {
		t.Fatalf("RabbitMQ username = %q, want guest", item.Username)
	}
	if item.RabbitMQHost != "127.0.0.1" || item.RabbitMQPort != 5672 || item.RabbitMQVHost != "/" {
		t.Fatalf("RabbitMQ context fields = %+v", item)
	}
}

func TestCtxAddedSubcommandHelp(t *testing.T) {
	tests := [][]string{
		{"ctx", "export", "--help"},
		{"ctx", "import", "--help"},
		{"ctx", "role", "--help"},
		{"ctx", "migrate-credentials", "--help"},
	}
	for _, args := range tests {
		out, err := runCommandForTest(t, args...)
		if err != nil {
			t.Fatalf("%v error = %v; out=%s", args, err, out)
		}
		if !strings.Contains(out, "Usage:") {
			t.Fatalf("%v help missing Usage: %s", args, out)
		}
	}
}

func TestCtxExportRedactsCredentialsByDefault(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{
		Base:                        corectx.Base{Password: "secret", CredentialBackend: "plain-yaml"},
		Backend:                     "kafka",
		KafkaBrokers:                []string{"127.0.0.1:9092"},
		KafkaSchemaRegistryPassword: "sr-secret",
		RabbitMQAMQPURL:             "amqp://user:url-pass@localhost:5672/",
	}); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "--config", configPath, "ctx", "export", "dev")
	if err != nil {
		t.Fatalf("ctx export error = %v; out=%s", err, out)
	}
	for _, forbidden := range []string{"secret", "sr-secret", "url-pass"} {
		if strings.Contains(out, forbidden) {
			t.Fatalf("ctx export leaked %q: %s", forbidden, out)
		}
	}
	if !strings.Contains(out, redactedCredential) || !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("ctx export missing redaction markers: %s", out)
	}
}

func TestCtxExportAllOutputAndImportInputOverwrite(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{Backend: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}}); err != nil {
		t.Fatal(err)
	}
	exportPath := filepath.Join(dir, "contexts.yaml")
	if out, err := runCommandForTest(t, "--config", configPath, "ctx", "export", "--all", "--output", exportPath); err != nil {
		t.Fatalf("ctx export --all error = %v; out=%s", err, out)
	}
	data, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "contexts:") || !strings.Contains(string(data), "dev:") {
		t.Fatalf("export --all output missing contexts: %s", data)
	}

	importPath := filepath.Join(dir, "imported.yaml")
	out, err := runCommandForTest(t, "--config", importPath, "-o", "json", "--yes", "ctx", "import", "--input", exportPath, "--overwrite")
	if err != nil {
		t.Fatalf("ctx import --input error = %v; out=%s", err, out)
	}
	if !strings.Contains(out, `"kind": "ContextImportResult"`) || !strings.Contains(out, `"count": 1`) {
		t.Fatalf("import output = %s", out)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contexts["dev"].Backend != "kafka" {
		t.Fatalf("imported context = %+v", cfg.Contexts["dev"])
	}
}

func TestCtxRoleLifecycle(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{Backend: "fake"}); err != nil {
		t.Fatal(err)
	}

	if err := runCtxRoleSet(newDefaultFlags(), "dev", roleOptions{targetOperator: "alice", role: safety.RoleReader}); err != nil {
		t.Fatalf("runCtxRoleSet error = %v", err)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contexts["dev"].Roles["alice"] != safety.RoleReader {
		t.Fatalf("roles = %#v", cfg.Contexts["dev"].Roles)
	}
	if err := runCtxRoleUnset(newDefaultFlags(), "dev", roleOptions{targetOperator: "alice"}); err != nil {
		t.Fatalf("runCtxRoleUnset error = %v", err)
	}
	cfg, err = mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contexts["dev"].Roles != nil {
		t.Fatalf("roles after unset = %#v, want nil", cfg.Contexts["dev"].Roles)
	}
}

func TestCtxMigrateCredentialsDryRunCountsPrimaryAndSchemaRegistry(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{
		Base:                        corectx.Base{Password: "secret"},
		Backend:                     "kafka",
		KafkaBrokers:                []string{"127.0.0.1:9092"},
		KafkaSchemaRegistryPassword: "sr-secret",
	}); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "--config", configPath, "-o", "json", "ctx", "migrate-credentials", "--dry-run")
	if err != nil {
		t.Fatalf("ctx migrate-credentials --dry-run error = %v; out=%s", err, out)
	}
	for _, want := range []string{`"kind": "CredentialMigration"`, `"dryRun": true`, `"credentials": 2`, `"dev"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("migrate dry-run output missing %q: %s", want, out)
		}
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contexts["dev"].Password != "secret" || cfg.Contexts["dev"].KafkaSchemaRegistryPassword != "sr-secret" {
		t.Fatalf("dry-run mutated credentials: %+v", cfg.Contexts["dev"])
	}
}

func TestRabbitMQUsesMQGOVPasswordForCurrentContext(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	t.Setenv("MQGOV_PASSWORD", "env-secret")
	server := rabbitMQManagementAuthServer(t, "rabbit-user", "env-secret")
	defer server.Close()

	if err := mqgovctx.Set("prod", mqgovctx.Context{
		Base:                  corectx.Base{Username: "rabbit-user"},
		Backend:               "rabbitmq",
		RabbitMQManagementURL: server.URL,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("prod"); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "--config", configPath, "-o", "json", "topic", "list")
	if err != nil {
		t.Fatalf("topic list with MQGOV_PASSWORD error = %v; out=%s", err, out)
	}
}

func TestRabbitMQUsesMQGOVPasswordForContextOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	t.Setenv("MQGOV_PASSWORD", "override-secret")
	server := rabbitMQManagementAuthServer(t, "prod-user", "override-secret")
	defer server.Close()

	if err := mqgovctx.Set("dev", mqgovctx.Context{
		Base:                  corectx.Base{Username: "dev-user", Password: "dev-secret"},
		Backend:               "rabbitmq",
		RabbitMQManagementURL: "http://127.0.0.1:1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Set("prod", mqgovctx.Context{
		Base:                  corectx.Base{Username: "prod-user"},
		Backend:               "rabbitmq",
		RabbitMQManagementURL: server.URL,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("dev"); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "--config", configPath, "--context", "prod", "-o", "json", "topic", "list")
	if err != nil {
		t.Fatalf("topic list with --context prod and MQGOV_PASSWORD error = %v; out=%s", err, out)
	}
}

func rabbitMQManagementAuthServer(t *testing.T, wantUser, wantPassword string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != wantUser || password != wantPassword {
			t.Errorf("BasicAuth() = %q/%q ok=%t, want %q/%q", user, password, ok, wantUser, wantPassword)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/api/queues/%2F" {
			t.Errorf("request = %s %s, want GET /api/queues/%%2F", r.Method, r.URL.EscapedPath())
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
}
