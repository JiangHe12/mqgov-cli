package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/safety"

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
	if !strings.Contains(text, "key-sha256") || !strings.Contains(text, "body-sha256") {
		t.Fatalf("audit log missing fingerprints: %s", text)
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

func runCommandForTest(t *testing.T, args ...string) (string, error) {
	t.Helper()
	t.Setenv("NO_COLOR", "1")
	home := t.TempDir()
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
