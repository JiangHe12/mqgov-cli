package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"
	"github.com/JiangHe12/opskit-core/v2/lockfile"
	"github.com/JiangHe12/opskit-core/v2/safety"
	"gopkg.in/yaml.v3"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func TestAuditQueryAndVerifyJSONEnvelope(t *testing.T) {
	path := privateMutationAuditPath(t)
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

func TestAuditVerifyHumanOutputIncludesV2SecurityFields(t *testing.T) {
	path := privateMutationAuditPath(t)
	if err := audit.AppendWithOptions(path, audit.Event{
		EventType: audit.EventType("mq.audit.verify.output"),
		Operator:  "tester@host",
		Status:    audit.StatusSuccess,
	}, audit.Options{}); err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"table", "plain"} {
		t.Run(format, func(t *testing.T) {
			out, err := runCommandForTest(t, "-o", format, "audit", "verify", "--path", path)
			if err != nil {
				t.Fatalf("audit verify error = %v; out=%s", err, out)
			}
			for _, want := range []string{
				"authenticated", "legacy", "encrypted", "integrity", "sequence",
				"checkpoint", "truncation", "lock",
			} {
				if !strings.Contains(strings.ToLower(out), want) {
					t.Fatalf("%s output missing %q: %s", format, want, out)
				}
			}
		})
	}
}

func TestAuditControlRotationDoesNotEnterTargetNamespace(t *testing.T) {
	target := privateMutationAuditPath(t)
	control := auditControlPath(target)
	for index := 0; index < 2; index++ {
		if err := audit.AppendWithOptions(control, audit.Event{
			Timestamp: time.Unix(int64(index+1), 0).UTC(),
			EventType: audit.EventType("mq.audit.prune.control"),
			Operator:  "tester@host",
			Status:    audit.StatusSuccess,
		}, audit.Options{MaxSizeBytes: 1}); err != nil {
			t.Fatalf("append control record %d: %v", index, err)
		}
	}
	controlRotations, err := audit.RotatedFiles(control)
	if err != nil || len(controlRotations) == 0 {
		t.Fatalf("control rotations = %v, error = %v; want at least one", controlRotations, err)
	}
	targetRotations, err := audit.RotatedFiles(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(targetRotations) != 0 {
		t.Fatalf("target rotations include control evidence: %v", targetRotations)
	}
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

func TestAuditPruneRequiresTrustedR3AuthorizationBeforeDeletion(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		roles map[string]string
		env   string
		args  []string
	}{
		{
			name:  "missing ticket",
			roles: map[string]string{operator: safety.RoleAdmin},
			args:  []string{"--yes", "--allow-audit-prune"},
		},
		{
			name:  "wrong allow flag",
			roles: map[string]string{operator: safety.RoleAdmin},
			args:  []string{"--yes", "--ticket", "OPS-1", "--allow-topic-delete"},
		},
		{
			name: "spoofed operator flag and environment",
			roles: map[string]string{
				operator:        safety.RoleReader,
				"spoofed-admin": safety.RoleAdmin,
			},
			env:  "spoofed-admin",
			args: []string{"--operator", "spoofed-admin", "--yes", "--ticket", "OPS-1", "--allow-audit-prune"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath, auditPath, rotated := prepareGovernedAuditPrune(t, tt.roles)
			t.Setenv(mqgovOperatorEnv, tt.env)
			t.Setenv(deprecatedMqgovOperatorEnv, tt.env)
			args := append([]string{"--config", configPath}, tt.args...)
			args = append(args, "audit", "prune", "--path", auditPath, "--keep-last", "0", "--confirm")
			_, runErr := runCommandForTest(t, args...)
			if got := apperrors.AsAppError(runErr).Code; got != apperrors.CodeAuthorizationRequired {
				t.Fatalf("error code = %s, want %s; err=%v", got, apperrors.CodeAuthorizationRequired, runErr)
			}
			assertFileExists(t, rotated)
		})
	}
}

func TestAuditPruneRequiresConfirmAndR3ConfirmationWithExactAllow(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name       string
		globalArgs []string
		pruneArgs  []string
		wantCode   apperrors.ErrorCode
		deleted    bool
	}{
		{
			name:       "yes without confirm previews",
			globalArgs: []string{"--yes", "--ticket", "OPS-1", "--allow-audit-prune"},
		},
		{
			name:       "confirm without yes is denied",
			globalArgs: []string{"--non-interactive", "--ticket", "OPS-1", "--allow-audit-prune"},
			pruneArgs:  []string{"--confirm"},
			wantCode:   apperrors.CodeAuthorizationRequired,
		},
		{
			name:       "confirm and yes delete",
			globalArgs: []string{"--yes", "--ticket", "OPS-1", "--allow-audit-prune"},
			pruneArgs:  []string{"--confirm"},
			deleted:    true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			configPath, auditPath, rotated := prepareGovernedAuditPrune(t, map[string]string{operator: safety.RoleAdmin})
			args := append([]string{"--config", configPath}, tt.globalArgs...)
			args = append(args, "audit", "prune", "--path", auditPath, "--keep-last", "0")
			args = append(args, tt.pruneArgs...)
			out, runErr := runCommandForTest(t, args...)
			if tt.wantCode == "" {
				if runErr != nil {
					t.Fatalf("audit prune error = %v; out=%s", runErr, out)
				}
			} else if got := apperrors.AsAppError(runErr).Code; got != tt.wantCode {
				t.Fatalf("error code = %s, want %s; err=%v; out=%s", got, tt.wantCode, runErr, out)
			}
			_, statErr := os.Stat(rotated)
			if tt.deleted && !os.IsNotExist(statErr) {
				t.Fatalf("rotated audit file was not deleted: %v", statErr)
			}
			if !tt.deleted && statErr != nil {
				t.Fatalf("rotated audit file changed: %v", statErr)
			}
		})
	}
}

func TestAuditPrunePreviewRunsBeforeAuthorizationAndPreservesCandidateOrder(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name       string
		globalArgs []string
		pruneArgs  []string
	}{
		{name: "dry-run"},
		{name: "global-plan", globalArgs: []string{"--plan", "--yes"}, pruneArgs: []string{"--confirm"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			configPath, auditPath, rotated := prepareGovernedAuditPruneSequence(
				t,
				map[string]string{
					operator:        safety.RoleReader,
					"spoofed-admin": safety.RoleAdmin,
				},
				3,
			)
			args := append([]string{"--config", configPath, "--operator", "spoofed-admin", "-o", "json"}, tt.globalArgs...)
			args = append(args, "audit", "prune", "--path", auditPath, "--keep-last", "1")
			args = append(args, tt.pruneArgs...)
			out, runErr := runCommandForTest(t, args...)
			if runErr != nil {
				t.Fatalf("preview reached authorization: %v; out=%s", runErr, out)
			}
			var payload struct {
				Data auditPruneResult `json:"data"`
			}
			if err := json.Unmarshal([]byte(out), &payload); err != nil {
				t.Fatalf("decode preview: %v; out=%s", err, out)
			}
			want := rotated[:2]
			if !payload.Data.DryRun || len(payload.Data.DeletedFiles) != len(want) {
				t.Fatalf("preview = %+v, want ordered candidates %v", payload.Data, want)
			}
			for i := range want {
				if payload.Data.DeletedFiles[i] != want[i] {
					t.Fatalf("candidate order = %v, want %v", payload.Data.DeletedFiles, want)
				}
			}
			for _, filePath := range rotated {
				assertFileExists(t, filePath)
			}
		})
	}
}

func TestAuditPruneRejectsInvalidAuthenticatedStateButDryRunStillListsCandidates(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name            string
		setup           func(t *testing.T, active, candidate string)
		wantCode        apperrors.ErrorCode
		wantPreviewCode apperrors.ErrorCode
	}{
		{
			name: "checkpoint",
			setup: func(t *testing.T, active, _ string) {
				t.Helper()
				if err := os.WriteFile(active+".checkpoint", []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "active envelope",
			setup: func(t *testing.T, active, _ string) {
				t.Helper()
				writeV2AuditEnvelope(t, active)
			},
		},
		{
			name: "candidate envelope",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeV2AuditEnvelope(t, candidate)
			},
		},
		{
			name: "duplicate top-level markers",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"apiVersion":"opskit-core.io/audit/v2","kind":"AuditEnvelope","apiVersion":"legacy/v1","kind":"Legacy"}`)
			},
		},
		{
			name: "apiVersion marker only",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"apiVersion":"opskit-core.io/audit/v2"}`)
			},
		},
		{
			name: "kind marker only",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"kind":"AuditEnvelope"}`)
			},
		},
		{
			name: "uppercase apiVersion marker",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"APIVERSION":"opskit-core.io/audit/v2"}`)
			},
		},
		{
			name: "escaped apiVersion key",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"api\u0056ersion":"opskit-core.io/audit/v2"}`)
			},
		},
		{
			name: "unicode case-folded kind key",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"\u212Aind":"AuditEnvelope"}`)
			},
		},
		{
			name: "malformed marker",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"apiVersion":"opskit-core.io/audit/v2"`)
			},
			wantCode: apperrors.CodeValidationFailed,
		},
		{
			name: "malformed unicode escape",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"eventType":"legacy","value":"\u12xz"}`)
			},
			wantCode: apperrors.CodeValidationFailed,
		},
		{
			name: "trailing JSON",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"eventType":"legacy"} trailing`)
			},
			wantCode: apperrors.CodeValidationFailed,
		},
		{
			name: "complete envelope shape with unknown markers",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				writeAuditTestLine(t, candidate, `{"apiVersion":"unknown","kind":"unknown","keyId":"k","sequence":1,"payloadEncoding":"json","payload":{},"mac":"m"}`)
			},
		},
		{
			name: "checkpoint directory",
			setup: func(t *testing.T, active, _ string) {
				t.Helper()
				if err := os.Mkdir(active+".checkpoint", 0o700); err != nil {
					t.Fatal(err)
				}
			},
			wantCode: apperrors.CodeLocalIOError,
		},
		{
			name: "checkpoint symlink",
			setup: func(t *testing.T, active, _ string) {
				t.Helper()
				if err := os.Symlink(filepath.Join(filepath.Dir(active), "missing-checkpoint"), active+".checkpoint"); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
			},
			wantCode: apperrors.CodeValidationFailed,
		},
		{
			name: "candidate directory",
			setup: func(t *testing.T, _, candidate string) {
				t.Helper()
				if err := os.Remove(candidate); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(candidate, 0o700); err != nil {
					t.Fatal(err)
				}
			},
			wantPreviewCode: apperrors.CodeValidationFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath, auditPath, candidate := prepareGovernedAuditPrune(t, map[string]string{operator: safety.RoleAdmin})
			tt.setup(t, auditPath, candidate)

			out, previewErr := runCommandForTest(t,
				"--config", configPath,
				"-o", "json",
				"audit", "prune", "--path", auditPath, "--keep-last", "0",
			)
			if tt.wantPreviewCode != "" {
				if got := apperrors.AsAppError(previewErr).Code; got != tt.wantPreviewCode {
					t.Fatalf("preview error code = %s, want %s; err=%v", got, tt.wantPreviewCode, previewErr)
				}
				assertFileExists(t, candidate)
				return
			}
			if previewErr != nil {
				t.Fatalf("v2 preview error = %v; out=%s", previewErr, out)
			}
			var preview struct {
				Data auditPruneResult `json:"data"`
			}
			if err := json.Unmarshal([]byte(out), &preview); err != nil {
				t.Fatalf("decode preview: %v; out=%s", err, out)
			}
			if !preview.Data.DryRun || len(preview.Data.DeletedFiles) != 1 || preview.Data.DeletedFiles[0] != candidate {
				t.Fatalf("v2 preview = %+v", preview.Data)
			}

			_, runErr := runCommandForTest(t,
				"--config", configPath,
				"--yes", "--ticket", "OPS-1", "--allow-audit-prune",
				"audit", "prune", "--path", auditPath, "--keep-last", "0", "--confirm",
			)
			wantCode := tt.wantCode
			if wantCode == "" {
				wantCode = apperrors.CodeValidationFailed
			}
			if got := apperrors.AsAppError(runErr).Code; got != wantCode {
				t.Fatalf("error code = %s, want %s; err=%v", got, wantCode, runErr)
			}
			assertFileExists(t, candidate)
		})
	}
}

func TestAuditPruneRejectsInvalidV2InRetainedRotation(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath, auditPath, rotated := prepareGovernedAuditPruneSequence(
		t,
		map[string]string{operator: safety.RoleAdmin},
		2,
	)
	candidate, retained := rotated[0], rotated[1]
	writeV2AuditEnvelope(t, retained)

	_, runErr := runCommandForTest(t,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-audit-prune",
		"audit", "prune", "--path", auditPath, "--keep-last", "1", "--confirm",
	)
	if got := apperrors.AsAppError(runErr).Code; got != apperrors.CodeValidationFailed {
		t.Fatalf("error code = %s, want %s; err=%v", got, apperrors.CodeValidationFailed, runErr)
	}
	assertFileExists(t, candidate)
	assertFileExists(t, retained)
}

func TestAuditPruneAuthenticatedV2AdvancesCheckpointAndDeletesPrefix(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath, auditPath, legacy := prepareGovernedAuditPrune(t, map[string]string{operator: safety.RoleAdmin})
	for _, path := range []string{auditPath, legacy} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < 3; index++ {
		if err := audit.AppendWithOptions(auditPath, audit.Event{
			EventType: audit.EventType("test.v2"),
			Operator:  "tester",
			Status:    audit.StatusSuccess,
		}, audit.Options{MaxSizeBytes: 1}); err != nil {
			t.Fatalf("AppendWithOptions(%d) error = %v", index, err)
		}
	}
	rotated, err := audit.RotatedFiles(auditPath)
	if err != nil || len(rotated) != 2 {
		t.Fatalf("RotatedFiles() = %v, error = %v; want 2", rotated, err)
	}

	if _, err := runCommandForTest(t,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-audit-prune",
		"audit", "prune", "--path", auditPath, "--keep-last", "1", "--confirm",
	); err != nil {
		t.Fatalf("authenticated prune error = %v", err)
	}
	if _, err := os.Stat(rotated[0]); !os.IsNotExist(err) {
		t.Fatalf("oldest rotation still exists: %v", err)
	}
	assertFileExists(t, rotated[1])
	verified, err := audit.Verify(auditPath, audit.VerifyOptions{})
	if err != nil || verified.HasProblems() {
		t.Fatalf("Verify() = %+v, error = %v", verified, err)
	}
}

func TestAuditPruneLongLineAndCandidateChangeFailClosed(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	t.Run("long line", func(t *testing.T) {
		configPath, auditPath, candidate := prepareGovernedAuditPrune(t, map[string]string{operator: safety.RoleAdmin})
		writeAuditTestLine(t, candidate, strings.Repeat("x", 4*1024*1024+1))
		_, runErr := runCommandForTest(t,
			"--config", configPath,
			"--yes", "--ticket", "OPS-1", "--allow-audit-prune",
			"audit", "prune", "--path", auditPath, "--keep-last", "0", "--confirm",
		)
		if got := apperrors.AsAppError(runErr).Code; got != apperrors.CodeLocalIOError {
			t.Fatalf("error code = %s, want %s; err=%v", got, apperrors.CodeLocalIOError, runErr)
		}
		assertFileExists(t, candidate)
	})
	t.Run("candidate set changed", func(t *testing.T) {
		_, auditPath, candidate := prepareGovernedAuditPrune(t, map[string]string{operator: safety.RoleAdmin})
		opts := auditPruneOptions{keepLast: 0}
		expected, preview, err := auditPrunePlan(auditPath, opts)
		if err != nil {
			t.Fatal(err)
		}
		opts.expectedFiles = expected
		added := auditPath + ".20260201-000000.log"
		writeAuditTestLine(t, added, `{}`)
		if _, started, err := deleteAuditPruneCandidates(auditPath, opts, preview); apperrors.AsAppError(err).Code != apperrors.CodeConflict || started {
			t.Fatalf("deleteAuditPruneCandidates() error = %v, started=%t; want CONFLICT before deletion", err, started)
		}
		assertFileExists(t, candidate)
		assertFileExists(t, added)
	})
}

func TestAuditPruneUsesNumericRotationOrderAndRejectsUnexpectedNames(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.log")
	paths := []string{
		auditPath + ".20260201-000000.10.log",
		auditPath + ".20260201-000000.2.log",
		auditPath + ".20260201-000000.log",
	}
	for _, path := range paths {
		writeAuditTestLine(t, path, `{}`)
	}
	got, err := strictAuditRotatedFiles(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{paths[2], paths[1], paths[0]}
	if !slices.Equal(got, want) {
		t.Fatalf("strictAuditRotatedFiles() = %v, want %v", got, want)
	}

	unexpected := auditPath + ".20260201-000000.backup.log"
	writeAuditTestLine(t, unexpected, `{}`)
	if _, err := auditPruneCandidates(auditPath, auditPruneOptions{keepLast: 0}); apperrors.AsAppError(err).Code != apperrors.CodeValidationFailed {
		t.Fatalf("unexpected rotation error = %v, want VALIDATION_FAILED", err)
	}
	assertFileExists(t, unexpected)
}

func TestAuditPruneRejectsGovernedStateTargetBeforeIntent(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath, _, _ := prepareGovernedAuditPrune(t, map[string]string{operator: safety.RoleAdmin})
	before, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	rotation := configPath + ".20260201-000000.log"
	writeAuditTestLine(t, rotation, `{}`)
	_, err = runCommandForTest(
		t,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-audit-prune",
		"audit", "prune", "--path", configPath, "--keep-last", "0", "--confirm",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeUsageError {
		t.Fatalf("governed-state prune error = %v, want USAGE_ERROR", err)
	}
	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(before, after) {
		t.Fatal("rejected prune changed context config")
	}
	assertFileExists(t, rotation)
}

func TestAuditPruneRejectsDefaultAuditAliasesAndArtifactNamespaces(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	defaultPath, err := audit.DefaultPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(defaultPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(defaultPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	canonical, err := normalizeAuditPruneTarget(
		newDefaultFlags(),
		filepath.Join(filepath.Dir(defaultPath), ".", filepath.Base(defaultPath)),
	)
	if err != nil || canonical != defaultPath {
		t.Fatalf("canonical default path = %q, error=%v; want %q", canonical, err, defaultPath)
	}

	hardlink := filepath.Join(filepath.Dir(defaultPath), "audit-hardlink.log")
	if err := os.Link(defaultPath, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	if _, err := normalizeAuditPruneTarget(newDefaultFlags(), hardlink); apperrors.AsAppError(err).Code != apperrors.CodeUsageError {
		t.Fatalf("hardlink alias error = %v, want USAGE_ERROR", err)
	}
	for _, path := range []string{
		defaultPath + ".checkpoint.tmp-attacker",
		defaultPath + ".hmac-key.tmp-attacker",
		defaultPath + ".20260201-000000.2.log",
	} {
		if _, err := normalizeAuditPruneTarget(newDefaultFlags(), path); apperrors.AsAppError(err).Code != apperrors.CodeUsageError {
			t.Fatalf("artifact namespace %q error = %v, want USAGE_ERROR", path, err)
		}
	}
}

func TestAuditPruneDurabilityFailureOutcomeIsUncertain(t *testing.T) {
	outcome := auditPruneMutationOutcome(
		1,
		1,
		apperrors.New(
			apperrors.CodeLocalIOError,
			"injected",
			&auditPruneDurabilityError{cause: apperrors.New(apperrors.CodeLocalIOError, "sync", nil)},
		),
	)
	if outcome.Status != audit.StatusFailed ||
		outcome.Succeeded != 0 ||
		outcome.Failed != 0 ||
		outcome.Skipped != 0 ||
		outcome.Uncertain != 1 {
		t.Fatalf("durability failure outcome = %+v, want one uncertain deletion", outcome)
	}
}

func TestAuditPruneRejectsInvalidV2BeforeWritingIntent(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath, auditPath, candidate := prepareGovernedAuditPrune(t, map[string]string{operator: safety.RoleAdmin})
	writeV2AuditEnvelope(t, auditPath)
	before, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runCommandForTest(
		t,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-audit-prune",
		"audit", "prune", "--path", auditPath, "--keep-last", "0", "--confirm",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
		t.Fatalf("v2 prune error = %v, want VALIDATION_FAILED", err)
	}
	after, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(before, after) {
		t.Fatal("v2 rejection appended an intent to authenticated evidence")
	}
	assertFileExists(t, candidate)
}

func TestAuditPruneLockOrder(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath, auditPath, candidate := prepareGovernedAuditPrune(t, map[string]string{operator: safety.RoleAdmin})
	lock := lockfile.New(auditPath)
	if err := lock.Acquire(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Release() })
	t.Setenv("OPSKIT_LOCK_TIMEOUT", "100ms")

	if out, err := runCommandForTest(t,
		"--config", configPath,
		"audit", "prune", "--path", auditPath, "--keep-last", "0",
	); err != nil {
		t.Fatalf("preview waited for audit lock: %v; out=%s", err, out)
	}
	_, err = runCommandForTest(t,
		"--config", configPath,
		"--yes", "--allow-audit-prune",
		"audit", "prune", "--path", auditPath, "--keep-last", "0", "--confirm",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeAuthorizationRequired {
		t.Fatalf("unauthorized confirm error = %v, want AUTHORIZATION_REQUIRED before lock", err)
	}
	_, err = runCommandForTest(t,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-audit-prune",
		"audit", "prune", "--path", auditPath, "--keep-last", "0", "--confirm",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("authorized confirm error = %v, want LOCAL_IO_ERROR from held audit lock", err)
	}
	assertFileExists(t, candidate)
}

func TestAuditPruneRechecksPolicyUnderContextLock(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath, auditPath, candidate := prepareGovernedAuditPrune(
		t,
		map[string]string{operator: safety.RoleAdmin},
	)
	configLock := lockfile.New(filepath.Join(filepath.Dir(configPath), "config"))
	if err := configLock.Acquire(); err != nil {
		t.Fatal(err)
	}
	lockHeld := true
	t.Cleanup(func() {
		if lockHeld {
			_ = configLock.Release()
		}
	})

	flags := newDefaultFlags()
	flags.Yes = true
	flags.NonInter = true
	flags.Ticket = "OPS-1"
	flags.AllowAuditPrune = true
	result := make(chan error, 1)
	go func() {
		result <- runAuditPrune(flags, auditPruneOptions{
			path:     auditPath,
			keepLast: 0,
			confirm:  true,
		})
	}()
	select {
	case err := <-result:
		t.Fatalf("audit prune did not wait for the context lock: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	item := cfg.Contexts["guarded"]
	item.Roles = map[string]string{operator: safety.RoleReader}
	cfg.Contexts["guarded"] = item
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := configLock.Release(); err != nil {
		t.Fatal(err)
	}
	lockHeld = false

	select {
	case err := <-result:
		if apperrors.AsAppError(err).Code != apperrors.CodeAuthorizationRequired {
			t.Fatalf("audit prune error = %v, want authorization recheck failure", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("audit prune did not finish after releasing the context lock")
	}
	assertFileExists(t, candidate)
}

func prepareGovernedAuditPrune(t *testing.T, roles map[string]string) (string, string, string) {
	t.Helper()
	configPath, auditPath, rotated := prepareGovernedAuditPruneSequence(t, roles, 1)
	return configPath, auditPath, rotated[0]
}

func prepareGovernedAuditPruneSequence(t *testing.T, roles map[string]string, count int) (string, string, []string) {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("guarded", mqgovctx.Context{
		Base:    corectx.Base{Roles: roles},
		Backend: "fake",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("guarded"); err != nil {
		t.Fatal(err)
	}
	auditPath := privateMutationAuditPath(t)
	if err := os.WriteFile(auditPath, []byte(validLegacyAuditTestLine+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rotated := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		filePath := auditPath + "." + []string{"20260101-000000", "20260201-000000", "20260301-000000"}[i-1] + ".log"
		if err := os.WriteFile(filePath, []byte(validLegacyAuditTestLine+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		rotated = append(rotated, filePath)
	}
	return configPath, auditPath, rotated
}

const validLegacyAuditTestLine = `{"timestamp":"2026-01-01T00:00:00Z","eventType":"test.read","operator":"tester","status":"success","context":{},"target":{}}`

func writeV2AuditEnvelope(t *testing.T, path string) {
	t.Helper()
	content := " { \"kind\": \"AuditEnvelope\", \"apiVersion\": \"opskit-core.io/audit/v2\" }\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeAuditTestLine(t *testing.T, path, line string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}
