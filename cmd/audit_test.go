package cmd

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/JiangHe12/opskit-core/audit"
)

func TestAuditQueryAndVerifyJSONEnvelope(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	path := filepath.Join(home, ".mqgov-cli", "audit.log")
	if err := audit.AppendWithOptions(path, audit.Event{EventType: auditEventTopic, Operator: "tester", Status: audit.StatusSuccess}, audit.Options{}); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "-o", "json", "audit", "query", "--path", path, "--limit", "10")
	if err != nil {
		t.Fatalf("audit query error = %v; out=%s", err, out)
	}
	assertJSONKind(t, out, "AuditQueryResult")

	out, err = runCommandForTest(t, "-o", "json", "audit", "verify", "--path", path, "--strict")
	if err != nil {
		t.Fatalf("audit verify error = %v; out=%s", err, out)
	}
	assertJSONKind(t, out, "AuditVerifyResult")
}

func assertJSONKind(t *testing.T, out, want string) {
	t.Helper()
	var payload struct {
		Kind    string `json:"kind"`
		Success bool   `json:"success"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; out=%s", err, out)
	}
	if payload.Kind != want || !payload.Success {
		t.Fatalf("payload = %+v, want kind=%s success=true", payload, want)
	}
}
