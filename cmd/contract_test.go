package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func TestContractJSONEnvelopeAndExitCodes(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantExit int
		wantKind string
	}{
		{name: "R0 topic list json envelope", args: []string{"-o", "json", "topic", "list"}, wantExit: 0, wantKind: "TopicList"},
		{name: "R1 produce requires yes", args: []string{"-o", "json", "message", "produce", "orders", "--key", "secret-key", "--body", "secret-body"}, wantExit: 8},
		{name: "R1 produce with yes", args: []string{"-o", "json", "--yes", "message", "produce", "orders", "--key", "secret-key", "--body", "secret-body"}, wantExit: 0, wantKind: "MessageProduceResult"},
		{name: "tail unsupported backend fails closed", args: []string{"-o", "json", "message", "tail", "orders", "--max-messages", "1"}, wantExit: 12},
		{name: "acl unsupported backend fails closed", args: []string{"-o", "json", "acl", "list"}, wantExit: 12},
		{name: "schema list is R0", args: []string{"-o", "json", "schema", "list"}, wantExit: 0, wantKind: "SchemaList"},
		{name: "schema describe is R0", args: []string{"-o", "json", "schema", "describe", "orders-value"}, wantExit: 0, wantKind: "SchemaDescription"},
		{name: "schema check is R0", args: []string{"-o", "json", "schema", "check", "orders-value", "--schema", `{"type":"record","name":"Order"}`}, wantExit: 0, wantKind: "SchemaCheckResult"},
		{name: "schema register new subject requires R1 authorization", args: []string{"-o", "json", "schema", "register", "invoices-value", "--schema", `{"type":"record","name":"Invoice"}`}, wantExit: 8},
		{name: "schema register new subject passes with yes", args: []string{"-o", "json", "--yes", "schema", "register", "invoices-value", "--schema", `{"type":"record","name":"Invoice"}`}, wantExit: 0, wantKind: "SchemaDescription"},
		{name: "schema register existing subject requires R2 ticket", args: []string{"-o", "json", "--yes", "schema", "register", "orders-value", "--schema", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`}, wantExit: 8},
		{name: "schema register existing subject passes with yes and ticket", args: []string{"-o", "json", "--yes", "--ticket", "OPS-1", "schema", "register", "orders-value", "--schema", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`}, wantExit: 0, wantKind: "SchemaDescription"},
		{name: "schema delete requires R3 authorization", args: []string{"-o", "json", "schema", "delete", "orders-value"}, wantExit: 8},
		{name: "schema delete requires specific allow flag", args: []string{"-o", "json", "--yes", "--ticket", "OPS-1", "schema", "delete", "orders-value"}, wantExit: 8},
		{name: "schema delete passes with schema delete allow flag", args: []string{"-o", "json", "--yes", "--ticket", "OPS-1", "--allow-schema-delete", "schema", "delete", "orders-value"}, wantExit: 0, wantKind: "SchemaDeleteResult"},
		{name: "schema unsupported backend fails closed", args: []string{"-o", "json", "--backend", "rabbitmq", "schema", "list"}, wantExit: 12},
		{name: "message peek rejects zero count", args: []string{"-o", "json", "message", "peek", "orders", "--count", "0"}, wantExit: 1},
		{name: "dlq list is R0", args: []string{"-o", "json", "dlq", "list"}, wantExit: 0, wantKind: "DLQList"},
		{name: "dlq peek is R0", args: []string{"-o", "json", "dlq", "peek", "orders.dlq", "--count", "1"}, wantExit: 0, wantKind: "DLQPeekResult"},
		{name: "dlq peek rejects negative count", args: []string{"-o", "json", "dlq", "peek", "orders.dlq", "--count", "-1"}, wantExit: 1},
		{name: "dlq redrive dry-run previews without high-risk authorization", args: []string{"-o", "json", "dlq", "redrive", "orders.dlq", "--target", "orders", "--dry-run"}, wantExit: 0, wantKind: "DLQRedriveResult"},
		{name: "dlq redrive real execution requires internal produce allow flag", args: []string{"-o", "json", "--yes", "--ticket", "OPS-1", "dlq", "redrive", "orders.dlq", "--target", "orders"}, wantExit: 8},
		{name: "dlq redrive real execution passes with internal produce allow flag", args: []string{"-o", "json", "--yes", "--ticket", "OPS-1", "--allow-internal-produce", "dlq", "redrive", "orders.dlq", "--target", "orders"}, wantExit: 0, wantKind: "DLQRedriveResult"},
		{name: "dlq purge dry-run previews without high-risk authorization", args: []string{"-o", "json", "dlq", "purge", "orders.dlq", "--dry-run"}, wantExit: 0, wantKind: "DLQPurgeResult"},
		{name: "dlq purge real execution still requires high-risk authorization", args: []string{"-o", "json", "dlq", "purge", "orders.dlq"}, wantExit: 8},
		{name: "reset dry-run previews without high-risk authorization", args: []string{"-o", "json", "group", "reset-offset", "billing", "orders", "--dry-run"}, wantExit: 0, wantKind: "OffsetPlan"},
		{name: "reset real execution still requires high-risk authorization", args: []string{"-o", "json", "group", "reset-offset", "billing", "orders"}, wantExit: 8},
		{name: "purge dry-run previews without high-risk authorization", args: []string{"-o", "json", "topic", "purge", "orders", "--dry-run"}, wantExit: 0, wantKind: "TopicPurgeResult"},
		{name: "purge real execution still requires high-risk authorization", args: []string{"-o", "json", "topic", "purge", "orders"}, wantExit: 8},
		{name: "internal produce requires R3 authorization", args: []string{"-o", "json", "--yes", "--ticket", "OPS-1", "message", "produce", "__consumer_offsets", "--body", "x"}, wantExit: 8},
		{name: "internal produce passes with specific allow flag", args: []string{"-o", "json", "--yes", "--ticket", "OPS-1", "--allow-internal-produce", "message", "produce", "__consumer_offsets", "--body", "x"}, wantExit: 0, wantKind: "MessageProduceResult"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := runCommandForTest(t, tt.args...)
			if got := apperrors.ExitCode(err); got != tt.wantExit {
				t.Fatalf("ExitCode() = %d, want %d; err=%v; out=%s", got, tt.wantExit, err, out)
			}
			if tt.wantKind != "" {
				var payload struct {
					APIVersion string `json:"apiVersion"`
					Kind       string `json:"kind"`
					Success    bool   `json:"success"`
				}
				if err := json.Unmarshal([]byte(out), &payload); err != nil {
					t.Fatalf("json.Unmarshal() error = %v; out=%s", err, out)
				}
				if payload.APIVersion != apiVersion || payload.Kind != tt.wantKind || !payload.Success {
					t.Fatalf("payload = %+v, want apiVersion=%s kind=%s success=true", payload, apiVersion, tt.wantKind)
				}
			}
		})
	}
}

func TestAuditDoesNotPersistMessagePlaintext(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	_, err := runCommandForTest(t, "-o", "json", "--yes", "message", "produce", "orders", "--key", "secret-key", "--body", "secret-body")
	if err != nil {
		t.Fatalf("produce error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(os.Getenv("HOME"), ".mqgov-cli", "audit.log"))
	if err != nil {
		t.Fatalf("ReadFile(audit.log) error = %v", err)
	}
	text := string(data)
	for _, forbidden := range []string{"secret-key", "secret-body"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("audit log contains plaintext %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, `"keyFingerprint":"sha256:`) ||
		!strings.Contains(text, `"bodyFingerprint":"sha256:`) ||
		!strings.Contains(text, `"keyBytes":10`) ||
		!strings.Contains(text, `"bodyBytes":11`) {
		t.Fatalf("audit log missing fingerprints: %s", text)
	}
}

func TestAuditDoesNotPersistSchemaPlaintext(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	schema := `{"type":"record","name":"SecretSchema"}`
	_, err := runCommandForTest(t, "-o", "json", "schema", "check", "orders-value", "--schema", schema)
	if err != nil {
		t.Fatalf("schema check error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(os.Getenv("HOME"), ".mqgov-cli", "audit.log"))
	if err != nil {
		t.Fatalf("ReadFile(audit.log) error = %v", err)
	}
	text := string(data)
	if strings.Contains(text, "SecretSchema") || strings.Contains(text, schema) {
		t.Fatalf("audit log contains schema plaintext: %s", text)
	}
	if !strings.Contains(text, `"kind":"ReadAuditRecord"`) ||
		!strings.Contains(text, `"payloadFingerprint":"sha256:`) ||
		!strings.Contains(text, `"payloadBytes":`) {
		t.Fatalf("audit log missing safe read-request fingerprint: %s", text)
	}
}

func TestAuditDoesNotPersistRegisteredSchemaPlaintext(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	schema := `{"type":"record","name":"SecretRegisteredSchema"}`
	_, err := runCommandForTest(t, "-o", "json", "--yes", "schema", "register", "secret-value", "--schema", schema)
	if err != nil {
		t.Fatalf("schema register error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(os.Getenv("HOME"), ".mqgov-cli", "audit.log"))
	if err != nil {
		t.Fatalf("ReadFile(audit.log) error = %v", err)
	}
	text := string(data)
	if strings.Contains(text, "SecretRegisteredSchema") || strings.Contains(text, schema) {
		t.Fatalf("audit log contains registered schema plaintext: %s", text)
	}
	if !strings.Contains(text, `"payloadFingerprint":"sha256:`) ||
		!strings.Contains(text, fmt.Sprintf(`"payloadBytes":%d`, len(schema))) {
		t.Fatalf("audit log missing schema payload fingerprint: %s", text)
	}
}

func TestAuditPruneDryRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	oldRotated := path + ".20240101-000000.log"
	newRotated := path + ".20240102-000000.log"
	writePrivateMutationAuditTestFile(t, oldRotated, []byte("{}\n"))
	writePrivateMutationAuditTestFile(t, newRotated, []byte("{}\n"))

	out, err := runCommandForTest(t, "-o", "json", "audit", "prune", "--path", path, "--keep-last", "1")
	if err != nil {
		t.Fatalf("audit prune dry-run error = %v; out=%s", err, out)
	}
	var payload struct {
		Kind string `json:"kind"`
		Data struct {
			DryRun       bool     `json:"dryRun"`
			DeletedFiles []string `json:"deletedFiles"`
			Count        int      `json:"count"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; out=%s", err, out)
	}
	if payload.Kind != "AuditPruneResult" || !payload.Data.DryRun || payload.Data.Count != 1 ||
		len(payload.Data.DeletedFiles) != 1 || !sameAuditTestPath(payload.Data.DeletedFiles[0], oldRotated) {
		t.Fatalf("audit prune payload = %+v", payload)
	}
	for _, filePath := range []string{oldRotated, newRotated} {
		if _, err := os.Stat(filePath); err != nil {
			t.Fatalf("dry-run removed %s: %v", filePath, err)
		}
	}
}

func TestMessageMirrorGovernanceContract(t *testing.T) {
	tests := []struct {
		name     string
		source   mqgovctx.Context
		target   mqgovctx.Context
		args     []string
		wantExit int
	}{
		{
			name:     "protected source denies cheap exfiltration",
			source:   mqgovctx.Context{Base: corectx.Base{Protected: true}, Backend: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}},
			target:   mqgovctx.Context{Backend: "fake"},
			args:     []string{"-o", "json", "message", "mirror", "orders", "--to-context", "dst", "--to-topic", "orders", "--limit", "1", "--dry-run"},
			wantExit: 8,
		},
		{
			name:     "target write requires authorization",
			source:   mqgovctx.Context{Backend: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}},
			target:   mqgovctx.Context{Backend: "fake"},
			args:     []string{"-o", "json", "message", "mirror", "orders", "--to-context", "dst", "--to-topic", "orders", "--limit", "1"},
			wantExit: 8,
		},
		{
			name:     "internal target requires allow internal produce",
			source:   mqgovctx.Context{Backend: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}},
			target:   mqgovctx.Context{Backend: "fake"},
			args:     []string{"-o", "json", "--yes", "--ticket", "OPS-1", "message", "mirror", "orders", "--to-context", "dst", "--to-topic", "__consumer_offsets", "--limit", "1"},
			wantExit: 8,
		},
		{
			name:     "wildcard target rejected",
			source:   mqgovctx.Context{Backend: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}},
			target:   mqgovctx.Context{Backend: "fake"},
			args:     []string{"-o", "json", "--yes", "message", "mirror", "orders", "--to-context", "dst", "--to-topic", "orders-*", "--limit", "1"},
			wantExit: 1,
		},
		{
			name:     "rabbitmq source fails closed",
			source:   mqgovctx.Context{Backend: "rabbitmq"},
			target:   mqgovctx.Context{Backend: "fake"},
			args:     []string{"-o", "json", "message", "mirror", "orders", "--to-context", "dst", "--to-topic", "orders", "--limit", "1", "--dry-run"},
			wantExit: 12,
		},
		{
			name:     "rocketmq source fails closed",
			source:   mqgovctx.Context{Backend: "rocketmq", RocketMQNameServers: []string{"127.0.0.1:9876"}},
			target:   mqgovctx.Context{Backend: "fake"},
			args:     []string{"-o", "json", "message", "mirror", "orders", "--to-context", "dst", "--to-topic", "orders", "--limit", "1", "--dry-run"},
			wantExit: 12,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			mqgovctx.SetConfigPath(configPath)
			if err := mqgovctx.Set("src", tt.source); err != nil {
				t.Fatalf("set source context: %v", err)
			}
			if err := mqgovctx.Set("dst", tt.target); err != nil {
				t.Fatalf("set target context: %v", err)
			}
			if err := mqgovctx.Use("src"); err != nil {
				t.Fatalf("use source context: %v", err)
			}
			out, err := runCommandForTest(t, tt.args...)
			if got := apperrors.ExitCode(err); got != tt.wantExit {
				t.Fatalf("ExitCode() = %d, want %d; err=%v; out=%s", got, tt.wantExit, err, out)
			}
		})
	}
}

func TestAuthorizeProtectedContextEscalatesR1ToTicket(t *testing.T) {
	f := newDefaultFlags()
	f.Yes = true
	meta := mqgovctx.Context{}
	meta.Protected = true
	if err := authorize(f, safety.R1, meta, ""); apperrors.AsAppError(err).Code != apperrors.CodeAuthorizationRequired {
		t.Fatalf("authorize() = %v, want authorization required", err)
	}
	f.Ticket = "OPS-1"
	if err := authorize(f, safety.R1, meta, ""); err != nil {
		t.Fatalf("authorize() with ticket error = %v", err)
	}
}

func TestListPatternFlagsRejectGlobMetacharacters(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "topic star", args: []string{"topic", "list", "--pattern", "orders-*"}},
		{name: "group question", args: []string{"group", "list", "--pattern", "billing?"}},
		{name: "DLQ character class", args: []string{"dlq", "list", "--pattern", "orders[12].dlq"}},
		{name: "schema star", args: []string{"schema", "list", "--pattern", "orders-*"}},
		{name: "fleet star", args: []string{"fleet", "topics", "--all", "--pattern", "orders-*"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runCommandForTest(t, tt.args...)
			appErr := apperrors.AsAppError(err)
			if appErr.Code != apperrors.CodeUsageError {
				t.Fatalf("error code = %s, want %s; err=%v", appErr.Code, apperrors.CodeUsageError, err)
			}
			if !strings.Contains(appErr.Message, "exact name") {
				t.Fatalf("error message = %q, want exact-name contract", appErr.Message)
			}
		})
	}
}

func TestRootVersionFlagUsesPackageName(t *testing.T) {
	SetVersionInfo("v0.0.0-test", "deadbeef", "2026-06-29")
	t.Cleanup(func() { SetVersionInfo("dev", "unknown", "unknown") })
	output, err := runCommandForTest(t, "--version")
	if err != nil {
		t.Fatalf("Execute() error = %v, output=%s", err, output)
	}
	if want := "mqgov-cli version v0.0.0-test\n"; output != want {
		t.Fatalf("--version output = %q, want %q", output, want)
	}
}

func runCommandForTest(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return runCommandForTestAtHome(t, t.TempDir(), args...)
}

func runCommandForTestAtHome(t *testing.T, home string, args ...string) (string, error) {
	t.Helper()
	t.Setenv("NO_COLOR", "1")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	cmd := NewRootCmd()
	cmd.SetArgs(args)
	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe() error = %v", err)
	}
	os.Stdout = writer
	runErr := cmd.Execute()
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatalf("Close(writer) error = %v", closeErr)
	}
	os.Stdout = oldStdout
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		t.Fatalf("Copy(stdout) error = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close(reader) error = %v", err)
	}
	return buf.String(), runErr
}
