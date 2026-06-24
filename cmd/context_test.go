package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/credstore"

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
