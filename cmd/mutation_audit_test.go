package cmd

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func privateMutationAuditPath(t *testing.T) string {
	t.Helper()
	home := filepath.Join(t.TempDir(), "private-home")
	secureMutationAuditTestAncestors(t, home)
	if err := createPrivateMutationAuditDirectory(home); err != nil {
		t.Fatalf("create private test home: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	directory := filepath.Join(home, "private-audit")
	secureMutationAuditTestAncestors(t, directory)
	if err := createPrivateMutationAuditDirectory(directory); err != nil {
		t.Fatalf("createPrivateMutationAuditDirectory() error = %v", err)
	}
	return filepath.Join(directory, "audit.log")
}

func TestMutationAuditIntentPrecedesTargetAndOutcome(t *testing.T) {
	var records []safeAuditRecord
	runtime := &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x42}, 16)),
	}
	f := newDefaultFlags()
	f.mutationAudit = runtime
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.change",
		Context:   mqgovctx.Context{},
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	})
	if err != nil {
		t.Fatalf("beginMutationAudit() error = %v", err)
	}
	if len(records) != 1 || records[0].Phase != mutationAuditPhaseIntent {
		t.Fatalf("records before target change = %+v, want one intent", records)
	}
	targetChanged := true
	if err := finishMutationAudit(handle, mutationAuditOutcome{}, nil); err != nil {
		t.Fatalf("finishMutationAudit() error = %v", err)
	}
	if !targetChanged || len(records) != 2 || records[1].Phase != mutationAuditPhaseOutcome {
		t.Fatalf("records after target change = %+v, targetChanged=%t", records, targetChanged)
	}
	if records[0].MutationID == "" || records[0].MutationID != records[1].MutationID {
		t.Fatalf("mutation ids = %q, %q; want same non-empty id", records[0].MutationID, records[1].MutationID)
	}
}

func TestMutationAuditRejectsNonRegularActivePathBeforeAppend(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	if err := os.Mkdir(auditPath, 0o700); err != nil {
		t.Fatal(err)
	}
	appendCalled := false
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			appendCalled = true
			return nil
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: cryptorand.Reader,
	}
	_, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.nonregular",
		AuditPath: auditPath,
	})
	if apperrors.AsAppError(err).Code != apperrors.CodeLocalIOError {
		t.Fatalf("beginMutationAudit() error = %v, want LOCAL_IO_ERROR", err)
	}
	if appendCalled {
		t.Fatal("audit append was called for a non-regular active path")
	}
}

func TestMutationAuditRejectsSymlinkActivePathBeforeAppend(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	target := filepath.Join(filepath.Dir(auditPath), "target.log")
	writePrivateMutationAuditTestFile(t, target, []byte("{}\n"))
	if err := os.Symlink(target, auditPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	appendCalled := false
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			appendCalled = true
			return nil
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: cryptorand.Reader,
	}
	_, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.symlink",
		AuditPath: auditPath,
	})
	if apperrors.AsAppError(err).Code != apperrors.CodeLocalIOError {
		t.Fatalf("beginMutationAudit() error = %v, want LOCAL_IO_ERROR", err)
	}
	if appendCalled {
		t.Fatal("audit append was called for a symlink active path")
	}
}

func TestMutationAuditDurablySyncsRotationCreatedByAppend(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	writePrivateMutationAuditTestFile(t, auditPath, []byte("{}\n"))
	f := newDefaultFlags()
	f.AuditMaxSize = 1
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.rotation",
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("beginMutationAudit() error = %v", err)
	}
	rotated, err := audit.RotatedFiles(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotated) == 0 {
		t.Fatal("audit append did not create the expected rotation")
	}
	if err := finishMutationAudit(handle, mutationAuditOutcome{}, nil); err != nil {
		t.Fatalf("finishMutationAudit() error = %v", err)
	}
}

func TestMutationAuditIntentFailurePreventsContextMutation(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", root)
	t.Setenv("USERPROFILE", root)
	configPath := filepath.Join(root, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })

	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			return apperrors.New(apperrors.CodeLocalIOError, "injected intent failure", nil)
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x24}, 16)),
	}
	command := newRootCmdWith(f)
	command.SetArgs([]string{
		"--config", configPath,
		"--backend", "kafka",
		"--yes",
		"--ticket", "OPS-123",
		"--allow-context-change",
		"--non-interactive",
		"ctx", "set", "blocked",
	})
	err := command.Execute()
	if err == nil || apperrors.AsAppError(err).Code != apperrors.CodeLocalIOError {
		t.Fatalf("ctx set error = %v, want LOCAL_IO_ERROR", err)
	}
	cfg, loadErr := mqgovctx.Load()
	if loadErr != nil {
		t.Fatalf("mqgovctx.Load() error = %v", loadErr)
	}
	if _, exists := cfg.Contexts["blocked"]; exists {
		t.Fatal("context changed even though mutation intent failed")
	}
}

func TestMutationAuditIntentSyncFailurePreventsContextMutation(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", root)
	t.Setenv("USERPROFILE", root)
	configPath := filepath.Join(root, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })

	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecordWithResult: func(path string, record safeAuditRecord, options audit.Options) (audit.AppendResult, error) {
			result, err := audit.AppendRecordWithResult(path, record, options)
			if err != nil {
				return result, err
			}
			result.State = audit.AppendCommitCommittedPostCommitError
			return result, apperrors.New(apperrors.CodeLocalIOError, "injected post-commit failure", nil)
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x27}, 16)),
	}
	command := newRootCmdWith(f)
	command.SetArgs([]string{
		"--config", configPath,
		"--backend", "kafka",
		"--yes",
		"--ticket", "OPS-124",
		"--allow-context-change",
		"--non-interactive",
		"ctx", "set", "blocked-sync",
	})
	err := command.Execute()
	if err == nil || apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("ctx set error = %v, want AUDIT_INCOMPLETE", err)
	}
	cfg, loadErr := mqgovctx.Load()
	if loadErr != nil {
		t.Fatalf("mqgovctx.Load() error = %v", loadErr)
	}
	if _, exists := cfg.Contexts["blocked-sync"]; exists {
		t.Fatal("context changed even though mutation intent fsync failed")
	}
	auditPath, pathErr := audit.DefaultPath()
	if pathErr != nil {
		t.Fatalf("audit.DefaultPath() error = %v", pathErr)
	}
	query, queryErr := audit.QueryRaw(auditPath, audit.Filter{})
	if queryErr != nil || len(query.Records) != 1 {
		t.Fatalf("QueryRaw(audit intent) = %+v, error = %v", query, queryErr)
	}
	var intent safeAuditRecord
	if unmarshalErr := json.Unmarshal([]byte(query.Records[0].Line), &intent); unmarshalErr != nil {
		t.Fatalf("json.Unmarshal(intent) error = %v", unmarshalErr)
	}
	spoolFiles, globErr := filepath.Glob(filepath.Join(mutationAuditSpoolPath(auditPath), "*.json"))
	if globErr != nil || len(spoolFiles) != 1 {
		t.Fatalf("spool files = %v, error = %v; want one compensating outcome", spoolFiles, globErr)
	}
	outcome, spoolErr := readMutationSpoolRecord(spoolFiles[0])
	if spoolErr != nil {
		t.Fatalf("readMutationSpoolRecord() error = %v", spoolErr)
	}
	if outcome.MutationID != intent.MutationID ||
		outcome.Phase != mutationAuditPhaseOutcome ||
		outcome.Status != audit.StatusFailed ||
		outcome.Outcome == nil ||
		outcome.Outcome.Skipped != 1 ||
		outcome.Outcome.ErrorCode != string(codeAuditIncomplete) {
		t.Fatalf("compensating outcome = %+v, intent = %+v", outcome, intent)
	}
}

func TestMutationAuditOutcomeFailureSpoolsAndReplaysBeforeNextIntent(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	var mu sync.Mutex
	var records []safeAuditRecord
	failOutcome := true
	runtime := &mutationAuditRuntime{
		appendRecord: func(path string, record safeAuditRecord, options audit.Options) error {
			mu.Lock()
			defer mu.Unlock()
			if failOutcome && record.Phase == mutationAuditPhaseOutcome {
				return apperrors.New(apperrors.CodeLocalIOError, "injected outcome failure", nil)
			}
			records = append(records, record)
			return audit.AppendRecord(path, record, options)
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 16)),
	}
	f := newDefaultFlags()
	f.mutationAudit = runtime
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.first",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("begin first mutation error = %v", err)
	}
	firstID := handle.id
	err = finishMutationAudit(handle, mutationAuditOutcome{}, nil)
	if err == nil || apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("finish first mutation error = %v, want AUDIT_INCOMPLETE", err)
	}
	spoolFiles, err := filepath.Glob(filepath.Join(mutationAuditSpoolPath(auditPath), "*.json"))
	if err != nil || len(spoolFiles) != 1 {
		t.Fatalf("spool files = %v, error = %v; want one durable outcome", spoolFiles, err)
	}
	spooled, err := readMutationSpoolRecord(spoolFiles[0])
	if err != nil {
		t.Fatalf("readMutationSpoolRecord() error = %v", err)
	}
	if spooled.MutationID != firstID || spooled.Phase != mutationAuditPhaseOutcome {
		t.Fatalf("spooled record = %+v, want first outcome", spooled)
	}

	mu.Lock()
	records = nil
	failOutcome = false
	runtime.random = bytes.NewReader(bytes.Repeat([]byte{0x22}, 16))
	mu.Unlock()
	second, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.second",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("begin second mutation error = %v", err)
	}
	mu.Lock()
	got := append([]safeAuditRecord(nil), records...)
	mu.Unlock()
	if len(got) != 2 ||
		got[0].MutationID != firstID ||
		got[0].Phase != mutationAuditPhaseOutcome ||
		got[1].MutationID != second.id ||
		got[1].Phase != mutationAuditPhaseIntent {
		t.Fatalf("replay/order records = %+v", got)
	}
	spoolFiles, err = filepath.Glob(filepath.Join(mutationAuditSpoolPath(auditPath), "*.json"))
	if err != nil || len(spoolFiles) != 0 {
		t.Fatalf("spool files after replay = %v, error = %v", spoolFiles, err)
	}
	if err := finishMutationAudit(second, mutationAuditOutcome{}, nil); err != nil {
		t.Fatalf("finish second mutation error = %v", err)
	}
}

func TestMutationAuditOutcomeWithPossibleCommitIsNotBlindlySpooled(t *testing.T) {
	for _, state := range []audit.AppendCommitState{
		audit.AppendCommitCommittedPostCommitError,
		audit.AppendCommitIndeterminate,
	} {
		t.Run(string(state), func(t *testing.T) {
			auditPath := privateMutationAuditPath(t)
			calls := 0
			f := newDefaultFlags()
			f.mutationAudit = &mutationAuditRuntime{
				appendRecordWithResult: func(_ string, _ safeAuditRecord, _ audit.Options) (audit.AppendResult, error) {
					calls++
					if calls == 1 {
						return audit.AppendResult{State: audit.AppendCommitCommitted}, nil
					}
					return audit.AppendResult{State: state}, apperrors.New(apperrors.CodeLocalIOError, "injected append failure", nil)
				},
				now:    func() time.Time { return time.Now().UTC() },
				random: bytes.NewReader(bytes.Repeat([]byte{0x31}, 16)),
			}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:    "mq.test.commit-state",
				Target:    audit.EventTarget{ResourceType: "test"},
				AuditPath: auditPath,
			})
			if err != nil {
				t.Fatalf("beginMutationAudit() error = %v", err)
			}
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, nil); apperrors.AsAppError(err).Code != codeAuditIncomplete {
				t.Fatalf("finishMutationAudit() error = %v, want AUDIT_INCOMPLETE", err)
			}
			files, err := filepath.Glob(filepath.Join(mutationAuditSpoolPath(auditPath), "*.json"))
			if err != nil || len(files) != 0 {
				t.Fatalf("possibly committed outcome was blindly spooled: files=%v error=%v", files, err)
			}
		})
	}
}

func TestMutationAuditIndeterminateReplayIsQuarantinedAndNotRetried(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecordWithResult: func(path string, record safeAuditRecord, options audit.Options) (audit.AppendResult, error) {
			if record.Phase == mutationAuditPhaseOutcome {
				return audit.AppendResult{State: audit.AppendCommitNotCommitted},
					apperrors.New(apperrors.CodeLocalIOError, "injected initial outcome failure", nil)
			}
			return audit.AppendRecordWithResult(path, record, options)
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x32}, 16)),
	}
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.indeterminate-replay",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("beginMutationAudit() error = %v", err)
	}
	if err := finishMutationAudit(handle, mutationAuditOutcome{}, nil); apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("finishMutationAudit() error = %v, want AUDIT_INCOMPLETE", err)
	}

	replayCalls := 0
	f.mutationAudit = &mutationAuditRuntime{
		appendRecordWithResult: func(_ string, _ safeAuditRecord, _ audit.Options) (audit.AppendResult, error) {
			replayCalls++
			return audit.AppendResult{State: audit.AppendCommitIndeterminate},
				apperrors.New(apperrors.CodeLocalIOError, "injected indeterminate replay", nil)
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x33}, 16)),
	}
	if _, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.blocked-after-indeterminate",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	}); apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("first replay error = %v, want AUDIT_INCOMPLETE", err)
	}
	if replayCalls != 1 {
		t.Fatalf("first replay calls = %d, want 1", replayCalls)
	}
	spoolPath := mutationAuditSpoolPath(auditPath)
	marked, err := filepath.Glob(filepath.Join(spoolPath, "*.json"+mutationAuditIndeterminateSuffix))
	if err != nil || len(marked) != 1 {
		t.Fatalf("indeterminate spool files = %v, error = %v; want one", marked, err)
	}

	replayCalls = 0
	f.mutationAudit.appendRecordWithResult = func(_ string, _ safeAuditRecord, _ audit.Options) (audit.AppendResult, error) {
		replayCalls++
		return audit.AppendResult{State: audit.AppendCommitCommitted}, nil
	}
	if _, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.still-blocked",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	}); apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("second replay error = %v, want AUDIT_INCOMPLETE", err)
	}
	if replayCalls != 0 {
		t.Fatalf("quarantined replay calls = %d, want 0", replayCalls)
	}
}

func TestMutationAuditOutcomeReplaysBeforeAlreadyStartedMutationOutcome(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	var records []safeAuditRecord
	firstOutcomeFailed := false
	firstID := ""
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			if record.MutationID == firstID &&
				record.Phase == mutationAuditPhaseOutcome &&
				!firstOutcomeFailed {
				firstOutcomeFailed = true
				return apperrors.New(apperrors.CodeLocalIOError, "injected first outcome failure", nil)
			}
			records = append(records, record)
			return nil
		},
		now: func() time.Time {
			return time.Unix(1700000000, int64(len(records))).UTC()
		},
		random: bytes.NewReader(append(
			bytes.Repeat([]byte{0x31}, 16),
			bytes.Repeat([]byte{0x32}, 16)...,
		)),
	}
	first, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.first-started",
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstID = first.id
	second, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.second-started",
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	records = nil
	if err := finishMutationAudit(first, mutationAuditOutcome{}, nil); apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("first outcome error = %v, want AUDIT_INCOMPLETE", err)
	}
	if err := finishMutationAudit(second, mutationAuditOutcome{}, nil); err != nil {
		t.Fatalf("second outcome error = %v", err)
	}
	if len(records) != 2 ||
		records[0].MutationID != first.id ||
		records[0].Phase != mutationAuditPhaseOutcome ||
		records[1].MutationID != second.id ||
		records[1].Phase != mutationAuditPhaseOutcome {
		t.Fatalf("outcome order = %+v, want replayed first outcome then second outcome", records)
	}
}

func TestQueuedSafeAuditRefreshesTimestampInAppendOrder(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	var records []safeAuditRecord
	var tick int64
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			return nil
		},
		now: func() time.Time {
			tick++
			return time.Unix(1700000000, tick).UTC()
		},
	}
	first := newSafeAuditRecord(
		f,
		audit.EventType("mq.test.first-read"),
		mqgovctx.Context{},
		"",
		audit.EventTarget{ResourceType: "test"},
		audit.StatusSuccess,
		"",
		nil,
	)
	second := newSafeAuditRecord(
		f,
		audit.EventType("mq.test.second-read"),
		mqgovctx.Context{},
		"",
		audit.EventTarget{ResourceType: "test"},
		audit.StatusSuccess,
		"",
		nil,
	)
	if err := appendQueuedAuditRecord(f, auditPath, second); err != nil {
		t.Fatal(err)
	}
	if err := appendQueuedAuditRecord(f, auditPath, first); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || !records[0].Timestamp.Before(records[1].Timestamp) {
		t.Fatalf("queued audit timestamps = %+v, want append-order timestamps", records)
	}
}

func TestSafeAuditRecordAndHistoricalQueryExcludeRawSecrets(t *testing.T) {
	secrets := []string{
		"ticket-secret-91",
		"reason-secret-92",
		"detail-secret-93",
		"error-secret-94",
		"message-secret-95",
		"body-secret-96",
		"command-secret-97",
		"stdout-secret-98",
		"stderr-secret-99",
	}
	f := newDefaultFlags()
	f.Ticket = secrets[0]
	f.Reason = secrets[1]
	record := newSafeAuditRecord(
		f,
		auditEventMessage,
		mqgovctx.Context{},
		"test",
		audit.EventTarget{ResourceType: "message", Resource: "orders"},
		audit.StatusFailed,
		secrets[2],
		apperrors.New(apperrors.CodeBackendError, secrets[3], nil),
	)
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	assertNoAuditSecrets(t, string(data), secrets[:4])
	for _, required := range []string{
		"ticketFingerprint",
		"ticketBytes",
		"reasonFingerprint",
		"reasonBytes",
		"detailFingerprint",
		"detailBytes",
		"errorFingerprint",
		"errorBytes",
		"errorCode",
	} {
		if !bytes.Contains(data, []byte(required)) {
			t.Fatalf("safe audit record missing %q: %s", required, data)
		}
	}

	legacy := map[string]any{
		"timestamp": time.Unix(1700000000, 0).UTC(),
		"eventType": "legacy.event",
		"operator":  "tester",
		"context":   map[string]any{"name": "legacy"},
		"ticket":    secrets[0],
		"reason":    secrets[1],
		"target":    map[string]any{"resourceType": "message", "resource": "orders"},
		"status":    audit.StatusFailed,
		"diff":      secrets[2],
		"error":     map[string]any{"code": "BACKEND_ERROR", "message": secrets[3]},
		"message":   secrets[4],
		"body":      secrets[5],
		"command":   secrets[6],
		"stdout":    secrets[7],
		"stderr":    secrets[8],
	}
	legacyData, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("json.Marshal(legacy) error = %v", err)
	}
	sanitized, err := sanitizeAuditRecord(legacyData)
	if err != nil {
		t.Fatalf("sanitizeAuditRecord() error = %v", err)
	}
	safeData, err := json.Marshal(sanitized)
	if err != nil {
		t.Fatalf("json.Marshal(sanitized) error = %v", err)
	}
	assertNoAuditSecrets(t, string(safeData), secrets)
}

func TestReplayRejectsUnexpectedSpoolEntry(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	if err := audit.AppendRecord(auditPath, safeAuditRecord{
		APIVersion: mutationAuditAPIVersion,
		Kind:       safeAuditKind,
		Event: audit.Event{
			Timestamp: time.Now().UTC(),
			EventType: auditEventDiagnostic,
			Status:    audit.StatusSuccess,
		},
	}, audit.Options{}); err != nil {
		t.Fatalf("audit.AppendRecord() error = %v", err)
	}
	spoolPath := mutationAuditSpoolPath(auditPath)
	if err := ensureMutationSpoolDirectory(spoolPath); err != nil {
		t.Fatalf("ensureMutationSpoolDirectory() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(spoolPath, "unexpected.txt"), []byte("unsafe"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	f := newDefaultFlags()
	_, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.blocked",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err == nil || apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("beginMutationAudit() error = %v, want AUDIT_INCOMPLETE", err)
	}
}

func TestEnsureMutationSpoolDirectoryConcurrentCreation(t *testing.T) {
	spoolPath := mutationAuditSpoolPath(privateMutationAuditPath(t))
	const workers = 8
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errs <- ensureMutationSpoolDirectory(spoolPath)
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ensureMutationSpoolDirectory() error = %v", err)
		}
	}
	if err := verifyMutationSpoolDirectory(spoolPath); err != nil {
		t.Fatalf("verifyMutationSpoolDirectory() error = %v", err)
	}
}

func TestMutationAuditConcurrentReplayIsOrderedAndSingle(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	initialRuntime := &mutationAuditRuntime{
		appendRecord: func(path string, record safeAuditRecord, options audit.Options) error {
			if record.Phase == mutationAuditPhaseOutcome {
				return apperrors.New(apperrors.CodeLocalIOError, "injected outcome failure", nil)
			}
			return audit.AppendRecord(path, record, options)
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x33}, 16)),
	}
	f := newDefaultFlags()
	f.mutationAudit = initialRuntime
	first, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.spooled",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("begin spooled mutation error = %v", err)
	}
	if err := finishMutationAudit(first, mutationAuditOutcome{}, nil); err == nil || apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("finish spooled mutation error = %v, want AUDIT_INCOMPLETE", err)
	}

	const workers = 3
	var mu sync.Mutex
	var records []safeAuditRecord
	runtime := &mutationAuditRuntime{
		appendRecord: func(path string, record safeAuditRecord, options audit.Options) error {
			if err := audit.AppendRecord(path, record, options); err != nil {
				return err
			}
			mu.Lock()
			records = append(records, record)
			mu.Unlock()
			return nil
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: cryptorand.Reader,
	}
	f.mutationAudit = runtime
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			handle, beginErr := beginMutationAudit(f, mutationAuditSpec{
				Action:    "mq.test.concurrent",
				Target:    audit.EventTarget{ResourceType: "test"},
				AuditPath: auditPath,
			})
			if beginErr != nil {
				errs <- beginErr
				return
			}
			errs <- finishMutationAudit(handle, mutationAuditOutcome{}, nil)
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent mutation error = %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(records) == 0 || records[0].MutationID != first.id || records[0].Phase != mutationAuditPhaseOutcome {
		t.Fatalf("first concurrent append = %+v, want replayed outcome before any new intent", records)
	}
	replayed := 0
	phasesByID := make(map[string]map[string]bool)
	for _, record := range records {
		if record.MutationID == first.id && record.Phase == mutationAuditPhaseOutcome {
			replayed++
		}
		if record.Action == "mq.test.concurrent" {
			if phasesByID[record.MutationID] == nil {
				phasesByID[record.MutationID] = make(map[string]bool)
			}
			phasesByID[record.MutationID][record.Phase] = true
		}
	}
	if replayed != 1 {
		t.Fatalf("replayed outcome count = %d, want 1", replayed)
	}
	if len(phasesByID) != workers {
		t.Fatalf("concurrent mutation ids = %d, want %d", len(phasesByID), workers)
	}
	for id, phases := range phasesByID {
		if !phases[mutationAuditPhaseIntent] || !phases[mutationAuditPhaseOutcome] {
			t.Fatalf("mutation %s phases = %v, want intent and outcome", id, phases)
		}
	}
}

func TestMutationAuditCrossProcessQueueReplaysBeforeNewIntent(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	runMutationAuditHelperProcess(t, auditPath, "seed", "spool")

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	commands := make([]*exec.Cmd, 0, 2)
	outputs := make([]*bytes.Buffer, 0, 2)
	for _, action := range []string{"follower-a", "follower-b"} {
		command := mutationAuditHelperCommand(t.Context(), executable, auditPath, action, "success")
		output := &bytes.Buffer{}
		command.Stdout = output
		command.Stderr = output
		if err := command.Start(); err != nil {
			t.Fatalf("start %s helper: %v", action, err)
		}
		commands = append(commands, command)
		outputs = append(outputs, output)
	}
	for index, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("helper %d error = %v; output=%s", index, err, outputs[index])
		}
	}

	records := readSafeAuditRecords(t, auditPath)
	if len(records) != 6 {
		t.Fatalf("cross-process audit records = %d, want 6: %+v", len(records), records)
	}
	seedID := records[0].MutationID
	if records[0].Action != "mq.test.seed" ||
		records[0].Phase != mutationAuditPhaseIntent ||
		records[1].MutationID != seedID ||
		records[1].Phase != mutationAuditPhaseOutcome {
		t.Fatalf("cross-process replay order = %+v", records)
	}
	seedOutcomes := 0
	for _, record := range records {
		if record.MutationID == seedID && record.Phase == mutationAuditPhaseOutcome {
			seedOutcomes++
		}
	}
	if seedOutcomes != 1 {
		t.Fatalf("seed outcome replay count = %d, want 1", seedOutcomes)
	}
	spoolFiles, err := filepath.Glob(filepath.Join(mutationAuditSpoolPath(auditPath), "*.json"))
	if err != nil || len(spoolFiles) != 0 {
		t.Fatalf("spool after cross-process replay = %v, error=%v", spoolFiles, err)
	}
}

func TestMutationAuditSubprocessHelper(t *testing.T) {
	auditPath := os.Getenv("MQGOV_TEST_MUTATION_AUDIT_PATH")
	if auditPath == "" {
		return
	}
	action := os.Getenv("MQGOV_TEST_MUTATION_AUDIT_ACTION")
	mode := os.Getenv("MQGOV_TEST_MUTATION_AUDIT_MODE")
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecordWithResult: func(path string, record safeAuditRecord, options audit.Options) (audit.AppendResult, error) {
			if mode == "spool" && record.Phase == mutationAuditPhaseOutcome {
				return audit.AppendResult{State: audit.AppendCommitNotCommitted}, apperrors.New(apperrors.CodeLocalIOError, "injected subprocess outcome failure", nil)
			}
			return audit.AppendRecordWithResult(path, record, options)
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: cryptorand.Reader,
	}
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test." + action,
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("subprocess begin mutation: %v", err)
	}
	err = finishMutationAudit(handle, mutationAuditOutcome{}, nil)
	if mode == "spool" {
		if apperrors.AsAppError(err).Code != codeAuditIncomplete {
			t.Fatalf("subprocess spool error = %v, want AUDIT_INCOMPLETE", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("subprocess finish mutation: %v", err)
	}
}

func runMutationAuditHelperProcess(t *testing.T, auditPath, action, mode string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := mutationAuditHelperCommand(t.Context(), executable, auditPath, action, mode)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s helper error = %v; output=%s", action, err, output)
	}
}

func mutationAuditHelperCommand(ctx context.Context, executable, auditPath, action, mode string) *exec.Cmd {
	command := exec.CommandContext(ctx, executable, "-test.run=^TestMutationAuditSubprocessHelper$", "-test.count=1") //nolint:gosec // Executable is the current signed test binary.
	command.Env = append(
		os.Environ(),
		"MQGOV_TEST_MUTATION_AUDIT_PATH="+auditPath,
		"MQGOV_TEST_MUTATION_AUDIT_ACTION="+action,
		"MQGOV_TEST_MUTATION_AUDIT_MODE="+mode,
	)
	return command
}

func TestMutationAuditOutcomeFallbackBlocksNextIntentUntilReplay(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	outcomeBlocked := make(chan struct{})
	releaseOutcome := make(chan struct{})
	secondIntent := make(chan struct{}, 1)
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseOutcome) })
	}
	defer release()

	var stateMu sync.Mutex
	failedOutcome := false
	var records []safeAuditRecord
	runtime := &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			stateMu.Lock()
			shouldFail := record.Phase == mutationAuditPhaseOutcome && !failedOutcome
			if shouldFail {
				failedOutcome = true
			}
			stateMu.Unlock()
			if shouldFail {
				close(outcomeBlocked)
				<-releaseOutcome
				return apperrors.New(apperrors.CodeLocalIOError, "injected first outcome failure", nil)
			}
			stateMu.Lock()
			records = append(records, record)
			stateMu.Unlock()
			if record.Action == "mq.test.next" && record.Phase == mutationAuditPhaseIntent {
				secondIntent <- struct{}{}
			}
			return nil
		},
		now: func() time.Time { return time.Now().UTC() },
		random: bytes.NewReader(append(
			bytes.Repeat([]byte{0x71}, 16),
			bytes.Repeat([]byte{0x72}, 16)...,
		)),
	}
	f := newDefaultFlags()
	f.mutationAudit = runtime
	first, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.first",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("begin first mutation error = %v", err)
	}

	finishResult := make(chan error, 1)
	go func() {
		finishResult <- finishMutationAudit(first, mutationAuditOutcome{}, nil)
	}()
	<-outcomeBlocked

	beginStarted := make(chan struct{})
	type beginResult struct {
		handle *mutationAuditHandle
		err    error
	}
	nextResult := make(chan beginResult, 1)
	go func() {
		close(beginStarted)
		handle, beginErr := beginMutationAudit(f, mutationAuditSpec{
			Action:    "mq.test.next",
			Target:    audit.EventTarget{ResourceType: "test"},
			AuditPath: auditPath,
		})
		nextResult <- beginResult{handle: handle, err: beginErr}
	}()
	<-beginStarted
	select {
	case <-secondIntent:
		t.Fatal("next intent was persisted while the prior outcome fallback was still pending")
	case <-time.After(100 * time.Millisecond):
	}

	release()
	if err := <-finishResult; err == nil || apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("finish first mutation error = %v, want AUDIT_INCOMPLETE", err)
	}
	next := <-nextResult
	if next.err != nil {
		t.Fatalf("begin next mutation error = %v", next.err)
	}

	stateMu.Lock()
	if len(records) < 3 ||
		records[0].MutationID != first.id ||
		records[0].Phase != mutationAuditPhaseIntent ||
		records[1].MutationID != first.id ||
		records[1].Phase != mutationAuditPhaseOutcome ||
		records[2].MutationID != next.handle.id ||
		records[2].Phase != mutationAuditPhaseIntent {
		got := append([]safeAuditRecord(nil), records...)
		stateMu.Unlock()
		t.Fatalf("mutation audit order = %+v, want first intent, replayed first outcome, then next intent", got)
	}
	stateMu.Unlock()
	if err := finishMutationAudit(next.handle, mutationAuditOutcome{}, nil); err != nil {
		t.Fatalf("finish next mutation error = %v", err)
	}
}

func TestMutationSpoolIsPrivateAndContainsNoRawSecrets(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	secret := "spool-secret-247"
	f := newDefaultFlags()
	f.Ticket = secret
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(path string, record safeAuditRecord, options audit.Options) error {
			if record.Phase == mutationAuditPhaseOutcome {
				return apperrors.New(apperrors.CodeLocalIOError, "injected outcome failure", nil)
			}
			return audit.AppendRecord(path, record, options)
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x55}, 16)),
	}
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.private-spool",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("beginMutationAudit() error = %v", err)
	}
	if err := finishMutationAudit(handle, mutationAuditOutcome{}, nil); err == nil || apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("finishMutationAudit() error = %v, want AUDIT_INCOMPLETE", err)
	}
	spoolPath := mutationAuditSpoolPath(auditPath)
	if err := verifyMutationSpoolDirectory(spoolPath); err != nil {
		t.Fatalf("verifyMutationSpoolDirectory() error = %v", err)
	}
	files, err := filepath.Glob(filepath.Join(spoolPath, "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("spool files = %v, error = %v", files, err)
	}
	if err := verifyMutationSpoolFile(files[0]); err != nil {
		t.Fatalf("verifyMutationSpoolFile() error = %v", err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("ReadFile(spool) error = %v", err)
	}
	if bytes.Contains(data, []byte(secret)) {
		t.Fatalf("spool contains raw secret %q: %s", secret, data)
	}
	if !bytes.Contains(data, []byte("ticketFingerprint")) || !bytes.Contains(data, []byte("ticketBytes")) {
		t.Fatalf("spool missing ticket fingerprint metadata: %s", data)
	}
}

func TestMutationSpoolSequenceDoesNotFollowWallClockRollback(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		now: func() time.Time { return time.Unix(1, 0).UTC() },
	}
	err := withMutationAuditQueue(auditPath, func(spoolPath string) error {
		for index, idByte := range []byte{0x81, 0x82} {
			record := safeAuditRecord{
				APIVersion: mutationAuditAPIVersion,
				Kind:       mutationAuditKind,
				Event: audit.Event{
					Timestamp: time.Unix(int64(2-index), 0).UTC(),
					EventType: audit.EventType("mq.test.outcome"),
					Status:    audit.StatusFailed,
				},
				MutationID: strings.Repeat(string([]byte{hexDigit(idByte >> 4), hexDigit(idByte & 0x0f)}), 16),
				Phase:      mutationAuditPhaseOutcome,
				Action:     "mq.test.sequence",
				Outcome: &mutationAuditOutcome{
					Status:  audit.StatusFailed,
					Skipped: 1,
					counted: true,
				},
			}
			if err := writeMutationSpoolRecord(f, spoolPath, record); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("write sequenced mutation outcomes error = %v", err)
	}
	files, err := filepath.Glob(filepath.Join(mutationAuditSpoolPath(auditPath), "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 ||
		!strings.HasPrefix(filepath.Base(files[0]), "00000000000000000001-") ||
		!strings.HasPrefix(filepath.Base(files[1]), "00000000000000000002-") {
		t.Fatalf("spool sequence files = %v", files)
	}
	sequenceData, err := os.ReadFile(filepath.Join(mutationAuditSpoolPath(auditPath), mutationAuditSpoolSequenceFile))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(sequenceData)) != "2" {
		t.Fatalf("persistent spool sequence = %q, want 2", sequenceData)
	}
}

func TestMutationSpoolRejectsInvalidSequenceAndRecordSemantics(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	firstID := strings.Repeat("11", 16)
	secondID := strings.Repeat("22", 16)
	err := withMutationAuditQueue(auditPath, func(spoolPath string) error {
		sequencePath := filepath.Join(spoolPath, mutationAuditSpoolSequenceFile)
		if err := writeMutationSpoolSequence(sequencePath, 2); err != nil {
			return err
		}
		duplicateNames := []string{
			"00000000000000000001-" + firstID + ".json",
			"00000000000000000001-" + secondID + ".json",
		}
		if err := validateMutationSpoolSequence(spoolPath, duplicateNames); err == nil {
			t.Fatal("validateMutationSpoolSequence() accepted a duplicate sequence")
		}
		if err := writeMutationSpoolSequence(sequencePath, 1); err != nil {
			return err
		}
		if err := validateMutationSpoolSequence(
			spoolPath,
			[]string{"00000000000000000002-" + secondID + ".json"},
		); err == nil {
			t.Fatal("validateMutationSpoolSequence() accepted a backward sequence file")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	newRecord := func() safeAuditRecord {
		return safeAuditRecord{
			APIVersion: mutationAuditAPIVersion,
			Kind:       mutationAuditKind,
			Event: audit.Event{
				Timestamp: time.Unix(1700000000, 0).UTC(),
				EventType: audit.EventType("mq.test.spool.outcome"),
				Status:    audit.StatusFailed,
			},
			MutationID: firstID,
			Phase:      mutationAuditPhaseOutcome,
			Action:     "mq.test.spool",
			Outcome: &mutationAuditOutcome{
				Status: audit.StatusFailed,
				Failed: 1,
			},
		}
	}
	validName := "00000000000000000001-" + firstID + ".json"
	if err := validateMutationSpoolRecord(newRecord(), validName); err != nil {
		t.Fatalf("valid spool record rejected: %v", err)
	}
	unsafeUncertain := newRecord()
	unsafeUncertain.Outcome.Failed = 0
	unsafeUncertain.Outcome.Uncertain = 1
	unsafeUncertain.Outcome.CompensationStatus = credentialCompensationNotSafe
	if err := validateMutationSpoolRecord(unsafeUncertain, validName); err != nil {
		t.Fatalf("valid unsafe uncertain compensation record rejected: %v", err)
	}
	tests := []struct {
		name   string
		file   string
		mutate func(*safeAuditRecord)
	}{
		{
			name: "filename mutation id",
			file: "00000000000000000001-" + secondID + ".json",
		},
		{
			name: "event type",
			mutate: func(record *safeAuditRecord) {
				record.EventType = "mq.test.other.outcome"
			},
		},
		{
			name: "status mismatch",
			mutate: func(record *safeAuditRecord) {
				record.Status = audit.StatusSuccess
			},
		},
		{
			name: "zero timestamp",
			mutate: func(record *safeAuditRecord) {
				record.Timestamp = time.Time{}
			},
		},
		{
			name: "unknown compensation state",
			mutate: func(record *safeAuditRecord) {
				record.Outcome.CompensationStatus = "mystery"
			},
		},
		{
			name: "partial without failure evidence",
			mutate: func(record *safeAuditRecord) {
				record.Status = audit.StatusPartialFailed
				record.Outcome.Status = audit.StatusPartialFailed
				record.Outcome.Succeeded = 1
				record.Outcome.Failed = 0
			},
		},
		{
			name: "planned item count mismatch",
			mutate: func(record *safeAuditRecord) {
				record.Metadata.Items = 2
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := newRecord()
			if tt.mutate != nil {
				tt.mutate(&record)
			}
			name := tt.file
			if name == "" {
				name = validName
			}
			if err := validateMutationSpoolRecord(record, name); err == nil {
				t.Fatalf("validateMutationSpoolRecord() accepted invalid record: %+v", record)
			}
		})
	}
	for _, data := range [][]byte{
		[]byte(`{"outcome":{"status":"failed","Status":"success"}}`),
		[]byte(`{"outcome":{"status":"failed","\u017ftatus":"success"}}`),
	} {
		if !hasDuplicateJSONKeyFold(data) {
			t.Fatalf("case-fold duplicate JSON key accepted: %s", data)
		}
	}
}

func TestCredentialMigrationPostCommitFailureSpoolsReplayableOutcome(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			if record.Phase == mutationAuditPhaseOutcome {
				return apperrors.New(apperrors.CodeLocalIOError, "injected outcome append failure", nil)
			}
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x91}, 16)),
	}
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.ctx.credentials.migrate",
		AuditPath: auditPath,
		Metadata:  mutationAuditMetadata{Items: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	operationErr := apperrors.New(apperrors.CodeLocalIOError, "config commit returned an error", nil)
	err = finishCredentialMigrationAudit(
		handle,
		2,
		credentialMigrationProgress{succeeded: 2},
		"not-safe",
		operationErr,
	)
	if apperrors.AsAppError(err).Code != codeAuditIncomplete {
		t.Fatalf("finishCredentialMigrationAudit() error = %v, want AUDIT_INCOMPLETE", err)
	}
	files, err := filepath.Glob(filepath.Join(mutationAuditSpoolPath(auditPath), "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("spooled outcomes = %v, error=%v", files, err)
	}
	record, err := readMutationSpoolRecord(files[0])
	if err != nil {
		t.Fatalf("readMutationSpoolRecord() error = %v", err)
	}
	if record.Status != audit.StatusPartialFailed ||
		record.Outcome == nil ||
		record.Outcome.Succeeded != 2 ||
		record.Outcome.Failed != 0 ||
		record.Outcome.CompensationStatus != "not-safe" {
		t.Fatalf("spooled credential outcome = %+v", record.Outcome)
	}
}

func hexDigit(value byte) byte {
	if value < 10 {
		return '0' + value
	}
	return 'a' + value - 10
}

func TestProduceCommandWritesPairedMutationRecords(t *testing.T) {
	if _, err := runCommandForTest(
		t,
		"-o", "json",
		"--yes",
		"message", "produce", "orders",
		"--key", "paired-key",
		"--body", "paired-body",
	); err != nil {
		t.Fatalf("message produce error = %v", err)
	}
	records := readSafeAuditRecords(t, filepath.Join(os.Getenv("HOME"), ".mqgov-cli", "audit.log"))
	if len(records) != 4 {
		t.Fatalf("audit records = %d, want 4: %+v", len(records), records)
	}
	if records[0].Kind != readAuditKind ||
		records[0].Action != "mq.message.produce.preflight" ||
		records[0].Phase != mutationAuditPhaseIntent ||
		records[1].Phase != mutationAuditPhaseOutcome ||
		records[0].OperationID == "" ||
		records[0].OperationID != records[1].OperationID ||
		records[2].Action != "mq.message.produce" ||
		records[2].Phase != mutationAuditPhaseIntent ||
		records[2].Status != audit.StatusPending ||
		records[3].Phase != mutationAuditPhaseOutcome ||
		records[3].Status != audit.StatusSuccess ||
		records[2].MutationID != records[3].MutationID ||
		records[3].Outcome == nil ||
		records[3].Outcome.Succeeded != 1 {
		t.Fatalf("paired mutation records = %+v", records)
	}
}

func TestFinishBatchMutationAuditPersistsCounts(t *testing.T) {
	var records []safeAuditRecord
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x66}, 16)),
	}
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.test.batch",
		Target:    audit.EventTarget{ResourceType: "message"},
		Metadata:  mutationAuditMetadata{Items: 5},
		AuditPath: privateMutationAuditPath(t),
	})
	if err != nil {
		t.Fatalf("beginMutationAudit() error = %v", err)
	}
	operationErr := apperrors.New(apperrors.CodeBackendError, "batch target failed", nil)
	if err := finishBatchMutationAuditWithOutcome(handle, 5, mqgov.BatchOutcome{Succeeded: 2, Failed: 1, Uncertain: 1}, operationErr); !errors.Is(err, operationErr) {
		t.Fatalf("finishBatchMutationAuditWithOutcome() error = %v, want original operation error", err)
	}
	outcome := records[len(records)-1].Outcome
	if outcome == nil ||
		outcome.Status != audit.StatusPartialFailed ||
		outcome.Succeeded != 2 ||
		outcome.Failed != 1 ||
		outcome.Uncertain != 1 ||
		outcome.Skipped != 1 ||
		outcome.ErrorCode != string(apperrors.CodeBackendError) {
		t.Fatalf("batch outcome = %+v", outcome)
	}
}

func TestAuditQuerySanitizesHistoricalRawFields(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	secrets := []string{
		"query-ticket-secret",
		"query-reason-secret",
		"query-message-secret",
		"query-body-secret",
		"query-command-secret",
		"query-stdout-secret",
		"query-stderr-secret",
	}
	legacy := map[string]any{
		"timestamp": time.Unix(1700000000, 0).UTC(),
		"eventType": "legacy.query",
		"operator":  "tester",
		"context":   map[string]any{"name": "legacy"},
		"ticket":    secrets[0],
		"reason":    secrets[1],
		"target":    map[string]any{"resourceType": "message"},
		"status":    audit.StatusSuccess,
		"message":   secrets[2],
		"body":      secrets[3],
		"command":   secrets[4],
		"stdout":    secrets[5],
		"stderr":    secrets[6],
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	writePrivateMutationAuditTestFile(t, auditPath, append(data, '\n'))
	out, err := runCommandForTest(t, "-o", "json", "audit", "query", "--path", auditPath)
	if err != nil {
		t.Fatalf("audit query error = %v; output=%s", err, out)
	}
	assertNoAuditSecrets(t, out, secrets)
	if !strings.Contains(out, "ticketFingerprint") ||
		!strings.Contains(out, "ticketBytes") ||
		!strings.Contains(out, "reasonFingerprint") ||
		!strings.Contains(out, "reasonBytes") {
		t.Fatalf("sanitized query output missing fingerprints: %s", out)
	}
}

func readSafeAuditRecords(t *testing.T, path string) []safeAuditRecord {
	t.Helper()
	query, err := audit.QueryRaw(path, audit.Filter{})
	if err != nil {
		t.Fatalf("QueryRaw(%s) error = %v", path, err)
	}
	records := make([]safeAuditRecord, 0, len(query.Records))
	for _, raw := range query.Records {
		var record safeAuditRecord
		if err := json.Unmarshal([]byte(raw.Line), &record); err != nil {
			t.Fatalf("decode audit record: %v; line=%s", err, raw.Line)
		}
		records = append(records, record)
	}
	return records
}

func TestAuditQueryRejectsInvalidFingerprintAndCountMetadata(t *testing.T) {
	validFingerprint := mutationAuditFingerprint("test", []byte("value"))
	tests := []struct {
		name   string
		fields map[string]any
	}{
		{name: "top-level fingerprint", fields: map[string]any{"ticketFingerprint": "sha256:xyz"}},
		{name: "top-level bytes", fields: map[string]any{"ticketFingerprint": validFingerprint, "ticketBytes": -1}},
		{name: "metadata fingerprint", fields: map[string]any{
			"metadata": map[string]any{"payloadFingerprint": "md5:bad", "payloadBytes": 1},
		}},
		{name: "metadata bytes", fields: map[string]any{
			"metadata": map[string]any{"keyFingerprint": validFingerprint, "keyBytes": -1},
		}},
		{name: "nested outcome fingerprint", fields: map[string]any{
			"outcome": map[string]any{
				"status":           audit.StatusFailed,
				"errorFingerprint": "sha256:1234",
				"errorBytes":       2,
			},
		}},
		{name: "nested outcome count", fields: map[string]any{
			"outcome": map[string]any{"status": audit.StatusFailed, "skipped": -1},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := map[string]any{
				"apiVersion": mutationAuditAPIVersion,
				"kind":       mutationAuditKind,
				"timestamp":  time.Unix(1700000000, 0).UTC(),
				"eventType":  "mq.test",
				"status":     audit.StatusSuccess,
			}
			for key, value := range tt.fields {
				record[key] = value
			}
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sanitizeAuditRecord(data); err == nil {
				t.Fatalf("sanitizeAuditRecord() accepted invalid metadata: %s", data)
			}
			result := sanitizeAuditQueryResult(audit.RawResult{
				Records: []audit.RawRecord{{Line: string(data)}},
			})
			if len(result.Records) != 0 || result.MalformedEntries != 1 {
				t.Fatalf("sanitized result = %+v, want one malformed entry", result)
			}
		})
	}
}

func TestTelemetryUsesOnlyTicketFingerprintAndErrorCode(t *testing.T) {
	ticket := "telemetry-ticket-secret-513"
	attrs := telemetryTicketAttributes(ticket)
	if len(attrs) != 2 {
		t.Fatalf("telemetry ticket attributes = %v, want fingerprint and byte length", attrs)
	}
	if string(attrs[0].Key) != "mqgov.ticket_fingerprint" ||
		attrs[0].Value.AsString() == ticket ||
		!strings.HasPrefix(attrs[0].Value.AsString(), "sha256:") {
		t.Fatalf("ticket fingerprint attribute = %v", attrs[0])
	}
	if string(attrs[1].Key) != "mqgov.ticket_bytes" || attrs[1].Value.AsInt64() != int64(len(ticket)) {
		t.Fatalf("ticket byte-length attribute = %v", attrs[1])
	}
	operationErr := apperrors.New(apperrors.CodeBackendError, "telemetry-error-secret-514", nil)
	if got := safeTelemetryErrorCode(operationErr); got != string(apperrors.CodeBackendError) {
		t.Fatalf("safeTelemetryErrorCode() = %q, want %q", got, apperrors.CodeBackendError)
	}
}

func assertNoAuditSecrets(t *testing.T, text string, secrets []string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(text, secret) {
			t.Fatalf("audit data contains raw secret %q: %s", secret, text)
		}
	}
}
