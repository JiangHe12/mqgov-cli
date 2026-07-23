package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"

	"github.com/JiangHe12/mqgov-cli/internal/backend/fake"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
	"github.com/JiangHe12/mqgov-cli/internal/tlspin"
)

func TestMandatoryReadIntentFailurePreventsBackendAccess(t *testing.T) {
	backendCalled := false
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			return errors.New("injected intent append failure")
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 16)),
	}

	result, err := runMandatoryRead(f, readAuditSpec{
		Action:    "mq.test.read",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	}, func() (string, error) {
		backendCalled = true
		return "must-not-run", nil
	}, func(string) int { return 1 })

	if backendCalled {
		t.Fatal("backend was accessed after mandatory read intent persistence failed")
	}
	if result != "" {
		t.Fatalf("result = %q, want zero value", result)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("runMandatoryRead() error = %v, code = %s, want LOCAL_IO_ERROR", err, got)
	}
}

func TestMandatoryBrokerReadIntentFailurePreventsBackendConstruction(t *testing.T) {
	buildCalls := 0
	f := newDefaultFlags()
	f.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			buildCalls++
			return fake.New("fake", ""), nil
		},
	}
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			return errors.New("injected intent append failure")
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x12}, 16)),
	}

	result, target, err := runMandatoryBrokerRead(f, readAuditSpec{
		Action:    "mq.test.broker-read",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	}, nil, func(mqgov.Broker, mqgovctx.Context) (string, error) {
		return "must-not-run", nil
	}, func(string) int {
		return 1
	})

	if buildCalls != 0 {
		t.Fatalf("backend build calls = %d, want 0 before durable intent", buildCalls)
	}
	if result != "" || target != (operationTarget{}) {
		t.Fatalf("result = %q target = %+v, want zero values", result, target)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("runMandatoryBrokerRead() error = %v, code = %s, want LOCAL_IO_ERROR", err, got)
	}
}

func TestMandatoryBrokerReadAuthorizesBeforeBackendConstruction(t *testing.T) {
	events := make([]string, 0, 5)
	f := newDefaultFlags()
	f.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			events = append(events, "build")
			return fake.New("fake", ""), nil
		},
	}
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			events = append(events, "audit:"+record.Phase)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(events))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x14}, 32)),
	}

	result, _, err := runMandatoryBrokerRead(f, readAuditSpec{
		Action:    "mq.test.broker-read-order",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	}, func(mqgovctx.Context) error {
		events = append(events, "authorize")
		return nil
	}, func(mqgov.Broker, mqgovctx.Context) (string, error) {
		events = append(events, "read")
		return "ok", nil
	}, func(string) int {
		return 1
	})

	if err != nil || result != "ok" {
		t.Fatalf("runMandatoryBrokerRead() result = %q error = %v", result, err)
	}
	want := []string{"audit:intent", "authorize", "build", "read", "audit:outcome"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestMandatoryBrokerReadAuthorizationFailurePreventsBackendConstruction(t *testing.T) {
	buildCalls := 0
	authorizeErr := apperrors.New(apperrors.CodeAuthorizationRequired, "injected authorization failure", nil)
	f := newDefaultFlags()
	f.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			buildCalls++
			return fake.New("fake", ""), nil
		},
	}
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error { return nil },
		now:          func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random:       bytes.NewReader(bytes.Repeat([]byte{0x15}, 32)),
	}

	_, _, err := runMandatoryBrokerRead(f, readAuditSpec{
		Action:    "mq.test.broker-read-authorization",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	}, func(mqgovctx.Context) error {
		return authorizeErr
	}, func(mqgov.Broker, mqgovctx.Context) (string, error) {
		return "must-not-run", nil
	}, func(string) int {
		return 1
	})

	if buildCalls != 0 {
		t.Fatalf("backend build calls = %d, want 0 after authorization failure", buildCalls)
	}
	if !errors.Is(err, authorizeErr) {
		t.Fatalf("runMandatoryBrokerRead() error = %v, want authorization cause", err)
	}
}

func TestMandatoryBrokerPreflightIntentFailurePreventsBackendConstruction(t *testing.T) {
	buildCalls := 0
	f := newDefaultFlags()
	f.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			buildCalls++
			return fake.New("fake", ""), nil
		},
	}
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			return errors.New("injected preflight intent failure")
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x16}, 16)),
	}

	result, err := runMandatoryBrokerPreflight(f, readAuditSpec{
		Action:    "mq.test.mutation.preflight",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	}, func(mqgov.Broker, mqgovctx.Context) (string, error) {
		return "must-not-run", nil
	}, func(string) int { return 1 })

	if buildCalls != 0 {
		t.Fatalf("backend build calls = %d, want 0 before durable preflight intent", buildCalls)
	}
	if result.Backend != nil || result.Value != "" {
		t.Fatalf("preflight result = %+v, want zero value", result)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeLocalIOError {
		t.Fatalf("preflight error = %v, code = %s, want LOCAL_IO_ERROR", err, code)
	}
}

func TestMandatoryBrokerPreflightOutcomeFailureSuppressesMutation(t *testing.T) {
	appendCalls := 0
	mutationCalls := 0
	f := newDefaultFlags()
	f.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			return fake.New("fake", ""), nil
		},
	}
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			appendCalls++
			if appendCalls == 2 {
				return errors.New("injected preflight outcome failure")
			}
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(appendCalls)).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x17}, 32)),
	}

	result, err := runMandatoryBrokerPreflight(f, readAuditSpec{
		Action:    "mq.test.mutation.preflight",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	}, func(mqgov.Broker, mqgovctx.Context) (string, error) {
		return "bounded-plan", nil
	}, func(string) int { return 1 })
	if err == nil {
		mutationCalls++
	}

	if mutationCalls != 0 {
		t.Fatalf("mutation calls = %d, want 0 after preflight outcome failure", mutationCalls)
	}
	if result.Backend != nil || result.Value != "" {
		t.Fatalf("preflight result = %+v, want zero value", result)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeLocalIOError {
		t.Fatalf("preflight error = %v, code = %s, want LOCAL_IO_ERROR", err, code)
	}
}

func TestMutationPreflightAuthorizationFailurePreventsBackendConstruction(t *testing.T) {
	writeFleetTestConfig(t, map[string]mqgovctx.Context{
		"denied": {Base: corectx.Base{Roles: map[string]string{"bob": "reader"}}, Backend: "fake"},
	})
	buildCalls := 0
	f := newDefaultFlags()
	f.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			buildCalls++
			return fake.New("fake", ""), nil
		},
	}

	output, err := executeReadAuditTestCommand(
		t,
		f,
		"-o", "json",
		"--context", "denied",
		"--operator", "alice",
		"--yes",
		"message", "produce", "orders",
		"--body", "must-not-produce",
	)

	if output != "" {
		t.Fatalf("mutation output = %q, want none", output)
	}
	if buildCalls != 0 {
		t.Fatalf("backend build calls = %d, want 0 after preflight authorization failure", buildCalls)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeAuthorizationRequired {
		t.Fatalf("mutation error = %v, code = %s, want AUTHORIZATION_REQUIRED", err, code)
	}
}

func TestEveryMutationPreflightIntentFailurePreventsBackendConstruction(t *testing.T) {
	for _, test := range mutationPreflightCommandCases() {
		t.Run(test.name, func(t *testing.T) {
			buildCalls := 0
			f := newDefaultFlags()
			f.brokerRuntime = &brokerCommandRuntime{
				buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
					buildCalls++
					return fake.New("fake", ""), nil
				},
			}
			f.mutationAudit = &mutationAuditRuntime{
				appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
					return errors.New("injected preflight intent failure")
				},
				now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
				random: bytes.NewReader(bytes.Repeat([]byte{0x18}, 16)),
			}

			output, err := executeReadAuditTestCommand(t, f, test.args...)

			if output != "" {
				t.Fatalf("output = %q, want none", output)
			}
			if buildCalls != 0 {
				t.Fatalf("backend build calls = %d, want 0", buildCalls)
			}
			if code := apperrors.AsAppError(err).Code; code != apperrors.CodeLocalIOError {
				t.Fatalf("error = %v, code = %s, want LOCAL_IO_ERROR", err, code)
			}
		})
	}
}

func TestEveryMutationPreflightAuthorizationFailurePreventsBackendConstruction(t *testing.T) {
	writeFleetTestConfig(t, map[string]mqgovctx.Context{
		"denied": {Base: corectx.Base{Roles: map[string]string{"bob": "reader"}}, Backend: "fake"},
	})
	for _, test := range mutationPreflightCommandCases() {
		t.Run(test.name, func(t *testing.T) {
			buildCalls := 0
			f := newDefaultFlags()
			f.brokerRuntime = &brokerCommandRuntime{
				buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
					buildCalls++
					return fake.New("fake", ""), nil
				},
			}
			args := append([]string{"--context", "denied", "--operator", "alice"}, test.args...)

			output, err := executeReadAuditTestCommand(t, f, args...)

			if output != "" {
				t.Fatalf("output = %q, want none", output)
			}
			if buildCalls != 0 {
				t.Fatalf("backend build calls = %d, want 0", buildCalls)
			}
			if code := apperrors.AsAppError(err).Code; code != apperrors.CodeAuthorizationRequired {
				t.Fatalf("error = %v, code = %s, want AUTHORIZATION_REQUIRED", err, code)
			}
		})
	}
}

type mutationPreflightCommandCase struct {
	name string
	args []string
}

func mutationPreflightCommandCases() []mutationPreflightCommandCase {
	return []mutationPreflightCommandCase{
		{name: "topic-create", args: []string{"topic", "create", "orders", "--partitions", "1"}},
		{name: "topic-alter", args: []string{"topic", "alter", "orders", "--partitions", "2"}},
		{name: "topic-delete", args: []string{"topic", "delete", "orders"}},
		{name: "topic-purge", args: []string{"--dry-run", "topic", "purge", "orders"}},
		{name: "group-create", args: []string{"group", "create", "workers"}},
		{name: "group-delete", args: []string{"group", "delete", "workers"}},
		{name: "offset-reset", args: []string{"--dry-run", "group", "reset-offset", "workers", "orders", "--to", "latest"}},
		{name: "message-produce", args: []string{"message", "produce", "orders", "--body", "body"}},
		{name: "dlq-redrive", args: []string{"--dry-run", "dlq", "redrive", "orders.dlq", "--target", "orders", "--count", "1"}},
		{name: "dlq-purge", args: []string{"--dry-run", "dlq", "purge", "orders.dlq"}},
		{name: "acl-grant", args: []string{"acl", "grant", "--principal", "User:svc", "--resource-type", "topic", "--resource-name", "orders", "--operation", "read", "--permission", "allow"}},
		{name: "acl-revoke", args: []string{"acl", "revoke", "--principal", "User:svc", "--resource-type", "topic", "--resource-name", "orders", "--operation", "read", "--permission", "allow"}},
		{name: "schema-register", args: []string{"schema", "register", "orders-value", "--schema-type", "AVRO", "--schema", "{}"}},
		{name: "schema-delete", args: []string{"schema", "delete", "orders-value"}},
	}
}

func TestDoctorIntentFailurePreventsBackendConstruction(t *testing.T) {
	buildCalls := 0
	f := newDefaultFlags()
	f.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			buildCalls++
			return fake.New("fake", ""), nil
		},
	}
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			return errors.New("injected intent append failure")
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x13}, 16)),
	}

	err := runDoctor(t.Context(), f)

	if buildCalls != 0 {
		t.Fatalf("backend build calls = %d, want 0 before durable doctor intent", buildCalls)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("runDoctor() error = %v, code = %s, want LOCAL_IO_ERROR", err, got)
	}
}

func TestMandatoryReadSuccessPersistsPairedRecordsBeforeReturningResult(t *testing.T) {
	const (
		requestSecret = "request-secret-value"
		resultSecret  = "result-secret-value"
	)
	var records []safeAuditRecord
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x22}, 16)),
	}

	result, err := runMandatoryRead(f, readAuditSpec{
		Action:    "mq.test.read",
		Target:    audit.EventTarget{ResourceType: "test"},
		Metadata:  mutationValueMetadata("mq.test.read", map[string]string{"filter": requestSecret}),
		AuditPath: privateMutationAuditPath(t),
	}, func() (string, error) {
		if len(records) != 1 || records[0].Phase != mutationAuditPhaseIntent {
			t.Fatalf("records at backend access = %+v, want one intent", records)
		}
		return resultSecret, nil
	}, func(string) int { return 3 })
	if err != nil {
		t.Fatalf("runMandatoryRead() error = %v", err)
	}
	if result != resultSecret {
		t.Fatalf("result = %q, want %q", result, resultSecret)
	}
	if len(records) != 2 {
		t.Fatalf("records = %+v, want intent and outcome", records)
	}
	if records[0].Kind != readAuditKind || records[1].Kind != readAuditKind ||
		records[0].OperationID == "" || records[0].OperationID != records[1].OperationID ||
		records[0].MutationID != "" || records[1].MutationID != "" {
		t.Fatalf("read audit identities = %+v, want paired operationId-only records", records)
	}
	if records[1].Outcome == nil ||
		records[1].Outcome.Status != audit.StatusSuccess ||
		records[1].Outcome.Succeeded != 1 ||
		records[1].Outcome.ResultCount != 3 {
		t.Fatalf("outcome = %+v, want success with resultCount=3", records[1].Outcome)
	}
	data, marshalErr := json.Marshal(records)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	serialized := string(data)
	if strings.Contains(serialized, requestSecret) || strings.Contains(serialized, resultSecret) {
		t.Fatalf("read audit leaked request or result content: %s", serialized)
	}
}

func TestMandatoryReadBackendFailurePersistsOutcomeAndPreservesError(t *testing.T) {
	operationErr := apperrors.New(apperrors.CodeBackendError, "injected backend failure", nil)
	var records []safeAuditRecord
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x33}, 16)),
	}

	result, err := runMandatoryRead(f, readAuditSpec{
		Action:    "mq.test.read",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	}, func() (string, error) {
		return "unreleased", operationErr
	}, func(string) int { return 1 })

	if result != "" {
		t.Fatalf("result = %q, want zero value on backend failure", result)
	}
	if !errors.Is(err, operationErr) {
		t.Fatalf("runMandatoryRead() error = %v, want original backend error", err)
	}
	if len(records) != 2 || records[1].Outcome == nil {
		t.Fatalf("records = %+v, want intent and failed outcome", records)
	}
	if records[1].Outcome.Status != audit.StatusFailed ||
		records[1].Outcome.Failed != 1 ||
		records[1].Outcome.ResultCount != 1 ||
		records[1].Outcome.ErrorCode != string(apperrors.CodeBackendError) {
		t.Fatalf("outcome = %+v, want one BACKEND_ERROR failure with one bounded partial result", records[1].Outcome)
	}
}

func TestMandatoryReadOutcomeFailureSuppressesResultAndPreservesOperationError(t *testing.T) {
	for _, test := range []struct {
		name         string
		operationErr error
	}{
		{name: "successful backend"},
		{name: "failed backend", operationErr: apperrors.New(apperrors.CodeBackendError, "backend failed", nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			appendCalls := 0
			f := newDefaultFlags()
			f.mutationAudit = &mutationAuditRuntime{
				appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
					appendCalls++
					if appendCalls == 2 {
						return errors.New("injected outcome append failure")
					}
					return nil
				},
				now:    func() time.Time { return time.Unix(1700000000, int64(appendCalls)).UTC() },
				random: bytes.NewReader(bytes.Repeat([]byte{0x44}, 16)),
			}

			result, err := runMandatoryRead(f, readAuditSpec{
				Action:    "mq.test.read",
				Target:    audit.EventTarget{ResourceType: "test"},
				AuditPath: privateMutationAuditPath(t),
			}, func() (string, error) {
				return "must-not-be-released", test.operationErr
			}, func(string) int { return 1 })

			if result != "" {
				t.Fatalf("result = %q, want zero value when outcome persistence fails", result)
			}
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
				t.Fatalf("runMandatoryRead() error = %v, code = %s, want LOCAL_IO_ERROR", err, got)
			}
			if test.operationErr != nil && !errors.Is(err, test.operationErr) {
				t.Fatalf("runMandatoryRead() error = %v, want backend error in cause chain", err)
			}
		})
	}
}

func TestReadCommandSuppressesOutputAndDiagnosticsWhenOutcomeAuditFails(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("NO_COLOR", "1")
	appendCalls := 0
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			appendCalls++
			if appendCalls == 2 {
				return errors.New("injected outcome append failure")
			}
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(appendCalls)).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x45}, 16)),
	}
	f.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(flags *cliFlags, _ mqgovctx.Context, _ string) (mqgov.Broker, error) {
			readTLSNotify(flags)(tlspin.Event{
				Address:     "broker.example:9093",
				Algorithm:   tlspin.Algorithm,
				Fingerprint: "SHA256:test",
			})
			return fake.New("fake", ""), nil
		},
	}

	stdout, stderr, err := executeReadAuditTestCommandStreams(t, f, "-o", "json", "topic", "list")

	if stdout != "" || stderr != "" {
		t.Fatalf("topic list stdout=%q stderr=%q, want neither result nor deferred diagnostics before a durable outcome", stdout, stderr)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("topic list error = %v, code = %s, want LOCAL_IO_ERROR", err, got)
	}
}

func TestReadDiagnosticFlushFollowsDurableOutcomeAndRedacts(t *testing.T) {
	const secret = "ghp_0123456789012345678901234567890123456789"
	var (
		events     []string
		diagnostic string
	)
	f := newDefaultFlags()
	f.readDiagnosticWrite = func(data []byte) (int, error) {
		events = append(events, "diagnostic")
		diagnostic += string(data)
		return len(data), nil
	}
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			events = append(events, "audit:"+record.Phase)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(events))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x46}, 16)),
	}

	result, err := runMandatoryRead(f, readAuditSpec{
		Action:    "mq.test.diagnostic",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: privateMutationAuditPath(t),
	}, func() (string, error) {
		f.readDiagnostics.appendStderr("token=" + secret + "\n")
		events = append(events, "read")
		return "ok", nil
	}, func(string) int { return 1 })
	if err != nil || result != "ok" {
		t.Fatalf("runMandatoryRead() result=%q error=%v", result, err)
	}
	want := []string{"audit:intent", "read", "audit:outcome", "diagnostic"}
	if !slices.Equal(events, want) {
		t.Fatalf("events=%v, want %v", events, want)
	}
	if strings.Contains(diagnostic, secret) || !strings.Contains(diagnostic, "[REDACTED]") {
		t.Fatalf("deferred diagnostic was not redacted before release: %q", diagnostic)
	}
}

func TestReadDiagnosticBufferIsBounded(t *testing.T) {
	buffer := &readDiagnosticBuffer{}
	buffer.appendStderr(strings.Repeat("x", maxReadDiagnosticBytes))
	buffer.appendStderr("overflow")
	if got := len(buffer.stderr); got != maxReadDiagnosticBytes {
		t.Fatalf("buffered diagnostic bytes=%d, want %d", got, maxReadDiagnosticBytes)
	}
	if !buffer.dropped {
		t.Fatal("diagnostic overflow was not recorded")
	}

	var released bytes.Buffer
	buffer.complete(true, released.Write)
	if got := released.Len(); got != maxReadDiagnosticBytes+len(readDiagnosticOverflowNotice) {
		t.Fatalf("released diagnostic bytes=%d, want %d", got, maxReadDiagnosticBytes+len(readDiagnosticOverflowNotice))
	}
	if !strings.HasSuffix(released.String(), readDiagnosticOverflowNotice) {
		t.Fatalf("overflow output does not end with the fixed notice")
	}
}

func TestReadDiagnosticBufferConcurrentCompletionIsSafe(t *testing.T) {
	t.Parallel()

	buffer := &readDiagnosticBuffer{}
	var writers sync.WaitGroup
	for range 32 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			buffer.appendStderr("diagnostic\n")
		}()
	}
	writers.Wait()

	var released bytes.Buffer
	buffer.complete(true, released.Write)
	releasedBytes := released.Len()
	buffer.appendStderr("late diagnostic")
	buffer.complete(true, released.Write)
	if released.Len() != releasedBytes {
		t.Fatalf("diagnostics changed after completion: %q", released.String()[releasedBytes:])
	}
}

func TestReadAuditOutcomeSpoolReplaysBeforeNextIntent(t *testing.T) {
	auditPath := privateMutationAuditPath(t)
	var records []safeAuditRecord
	failOutcome := true
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(path string, record safeAuditRecord, options audit.Options) error {
			if failOutcome && record.Phase == mutationAuditPhaseOutcome {
				return errors.New("injected outcome append failure")
			}
			records = append(records, record)
			return audit.AppendRecord(path, record, options)
		},
		now:    func() time.Time { return time.Now().UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x66}, 16)),
	}

	first, err := beginReadAudit(f, readAuditSpec{
		Action:    "mq.test.first-read",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("beginReadAudit() error = %v", err)
	}
	firstID := first.mutation.id
	if err := finishReadAudit(first, 1, nil); apperrors.AsAppError(err).Code != apperrors.CodeLocalIOError {
		t.Fatalf("finishReadAudit() error = %v, want LOCAL_IO_ERROR", err)
	}
	spoolFiles, err := filepath.Glob(filepath.Join(mutationAuditSpoolPath(auditPath), "*.json"))
	if err != nil || len(spoolFiles) != 1 {
		t.Fatalf("spool files = %v, error = %v; want one read outcome", spoolFiles, err)
	}
	spooled, err := readMutationSpoolRecord(spoolFiles[0])
	if err != nil {
		t.Fatalf("readMutationSpoolRecord() error = %v", err)
	}
	if spooled.Kind != readAuditKind ||
		spooled.OperationID != firstID ||
		spooled.MutationID != "" ||
		spooled.Phase != mutationAuditPhaseOutcome {
		t.Fatalf("spooled record = %+v, want operationId-only read outcome", spooled)
	}

	records = nil
	failOutcome = false
	f.mutationAudit.random = bytes.NewReader(bytes.Repeat([]byte{0x77}, 16))
	second, err := beginReadAudit(f, readAuditSpec{
		Action:    "mq.test.second-read",
		Target:    audit.EventTarget{ResourceType: "test"},
		AuditPath: auditPath,
	})
	if err != nil {
		t.Fatalf("begin second read error = %v", err)
	}
	if len(records) != 2 ||
		records[0].OperationID != firstID ||
		records[0].Phase != mutationAuditPhaseOutcome ||
		records[1].OperationID != second.mutation.id ||
		records[1].Phase != mutationAuditPhaseIntent {
		t.Fatalf("replay/order records = %+v", records)
	}
	if err := finishReadAudit(second, 1, nil); err != nil {
		t.Fatalf("finish second read error = %v", err)
	}
}

func TestMandatoryReadBatchPersistsBoundedFanoutCounts(t *testing.T) {
	var records []safeAuditRecord
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x55}, 16)),
	}
	handle, err := beginReadAudit(f, readAuditSpec{
		Action:    "mq.test.fanout",
		Target:    audit.EventTarget{ResourceType: "fleet"},
		Metadata:  mutationAuditMetadata{Items: 4},
		AuditPath: privateMutationAuditPath(t),
	})
	if err != nil {
		t.Fatalf("beginReadAudit() error = %v", err)
	}
	if err := finishReadAuditBatch(handle, 2, 1, 17, nil); err != nil {
		t.Fatalf("finishReadAuditBatch() error = %v", err)
	}

	if len(records) != 2 || records[0].OperationID == "" ||
		records[0].OperationID != records[1].OperationID ||
		records[1].Outcome == nil {
		t.Fatalf("records = %+v, want paired fanout audit records", records)
	}
	outcome := records[1].Outcome
	if outcome.Status != audit.StatusPartialFailed ||
		outcome.Succeeded != 2 ||
		outcome.Failed != 1 ||
		outcome.Skipped != 1 ||
		outcome.ResultCount != 17 {
		t.Fatalf("outcome = %+v, want bounded partial fanout counts", outcome)
	}
}

func executeReadAuditTestCommand(t *testing.T, f *cliFlags, args ...string) (string, error) {
	t.Helper()
	stdout, _, err := executeReadAuditTestCommandStreams(t, f, args...)
	return stdout, err
}

func executeReadAuditTestCommandStreams(t *testing.T, f *cliFlags, args ...string) (string, string, error) {
	t.Helper()
	command := newRootCmdWith(f)
	command.SetArgs(args)
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		t.Fatal(err)
	}
	os.Stdout = stdoutWriter
	os.Stderr = stderrWriter
	runErr := command.Execute()
	if closeErr := stdoutWriter.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if closeErr := stderrWriter.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	os.Stdout = oldStdout
	os.Stderr = oldStderr
	var stdout strings.Builder
	if _, err := io.Copy(&stdout, stdoutReader); err != nil {
		t.Fatal(err)
	}
	if err := stdoutReader.Close(); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	if _, err := io.Copy(&stderr, stderrReader); err != nil {
		t.Fatal(err)
	}
	if err := stderrReader.Close(); err != nil {
		t.Fatal(err)
	}
	return stdout.String(), stderr.String(), runErr
}
