package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"

	"github.com/JiangHe12/mqgov-cli/internal/backend/fake"
	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
	"github.com/JiangHe12/mqgov-cli/internal/tlspin"
)

func TestMirrorAuthorizationsBindIndependentContextNames(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	flags := newDefaultFlags()
	flags.Context = "src"
	flags.Yes = true
	flags.Ticket = "OPS-1"

	assertValidatorContext := func(path, want string, authorize func() error) {
		t.Helper()
		t.Setenv("MQGOV_TEST_VALIDATOR_CAPTURE", path)
		if err := authorize(); err != nil {
			t.Fatalf("authorize %s error = %v", want, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]string
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Fatalf("validator payload decode error = %v; payload=%s", err, data)
		}
		if payload["context"] != want {
			t.Fatalf("validator context = %q, want %q; payload=%+v", payload["context"], want, payload)
		}
	}

	sourceMeta := mqgovctx.Context{Base: corectx.Base{Protected: true, TicketValidator: executable}}
	assertValidatorContext(filepath.Join(t.TempDir(), "source.json"), "src", func() error {
		return classifyAndAuthorize(flags, sourceMeta, mqclass.OperationPeek, mqclass.Target{Topic: "orders", ProtectedTopic: true}, "")
	})

	targetFlags := fleetLocalFlags(flags, "dst")
	targetMeta := mqgovctx.Context{Base: corectx.Base{TicketValidator: executable}}
	assertValidatorContext(filepath.Join(t.TempDir(), "target.json"), "dst", func() error {
		return classifyAndAuthorize(&targetFlags, targetMeta, mqclass.OperationMirror, mqclass.Target{Topic: "payments", ProtectedTopic: true}, "")
	})
}

func TestMirrorSourceProtectionDimensionsEachEscalateOnce(t *testing.T) {
	tests := []struct {
		name       string
		meta       mqgovctx.Context
		target     mqclass.Target
		wantTicket bool
	}{
		{
			name: "protected context only is R1",
			meta: mqgovctx.Context{Base: corectx.Base{Protected: true}, Backend: "fake"},
			target: mqclass.Target{
				Backend: "fake",
				Topic:   "orders",
			},
		},
		{
			name: "protected topic only is R1",
			meta: mqgovctx.Context{Backend: "fake"},
			target: mqclass.Target{
				Backend:        "fake",
				Topic:          "orders",
				ProtectedTopic: true,
			},
		},
		{
			name:       "protected context and topic are R2",
			meta:       mqgovctx.Context{Base: corectx.Base{Protected: true}, Backend: "fake"},
			wantTicket: true,
			target: mqclass.Target{
				Backend:        "fake",
				Topic:          "orders",
				ProtectedTopic: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := newDefaultFlags()
			flags.NonInter = true
			if err := authorizeMirrorSource(flags, tt.meta, tt.target); apperrors.AsAppError(err).Code != apperrors.CodeAuthorizationRequired {
				t.Fatalf("authorization without confirmation error = %v, want authorization required", err)
			}
			flags.Yes = true
			err := authorizeMirrorSource(flags, tt.meta, tt.target)
			if !tt.wantTicket {
				if err != nil {
					t.Fatalf("R1 authorization with confirmation error = %v", err)
				}
				return
			}
			if apperrors.AsAppError(err).Code != apperrors.CodeAuthorizationRequired {
				t.Fatalf("R2 authorization without ticket error = %v, want authorization required", err)
			}
			flags.Ticket = "OPS-1"
			if err := authorizeMirrorSource(flags, tt.meta, tt.target); err != nil {
				t.Fatalf("R2 authorization with ticket error = %v", err)
			}
		})
	}
}

func TestMirrorAuditsBindSourceAndDestinationSeparately(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	flags := newDefaultFlags()
	flags.Context = "src"
	request := mqgov.MessageMirrorRequest{
		Source: mqgov.TopicCoordinate{Cluster: "source-cluster", Topic: "orders"},
		Target: mqgov.TopicCoordinate{Cluster: "target-cluster", Topic: "payments"},
		From:   "earliest",
		Limit:  2,
		DryRun: true,
	}
	result := mqgov.MessageMirrorResult{
		Source:      request.Source,
		Target:      request.Target,
		DryRun:      true,
		Count:       2,
		Impact:      []mqgov.PartitionImpact{{Partition: 0, Count: 2}},
		Fingerprint: mqgov.ResourceFingerprints{BodySHA256: strings.Repeat("a", 64), Count: 2, Size: 12},
	}

	sourceMeta := mqgovctx.Context{Backend: "fake"}
	handle, err := beginReadAudit(flags, mirrorSourceReadAuditSpec(
		"src",
		sourceMeta,
		request.Source.Topic,
		"dst",
		request.Target.Topic,
		request.From,
		request.Partition,
		request.Limit,
		request.DryRun,
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := finishReadAudit(handle, int(result.Count), nil); err != nil {
		t.Fatal(err)
	}
	appendMirrorTargetPreviewAudit(flags, mqgovctx.Context{Backend: "fake"}, "dst", request, result, audit.StatusSuccess, nil)

	path, err := audit.DefaultPath()
	if err != nil {
		t.Fatal(err)
	}
	query, err := audit.QueryRaw(path, audit.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Records) != 3 {
		t.Fatalf("audit record count = %d, want source intent/outcome and target preview", len(query.Records))
	}
	records := make([]safeAuditRecord, 0, 3)
	for _, raw := range query.Records {
		var record safeAuditRecord
		if err := json.Unmarshal([]byte(raw.Line), &record); err != nil {
			t.Fatalf("audit decode error = %v; line=%s", err, raw.Line)
		}
		records = append(records, record)
	}
	if records[0].Context.Name != "src" ||
		records[0].Target.Resource != "orders" ||
		records[0].Kind != readAuditKind ||
		records[0].Phase != mutationAuditPhaseIntent ||
		records[1].Phase != mutationAuditPhaseOutcome ||
		records[0].OperationID == "" ||
		records[0].OperationID != records[1].OperationID {
		t.Fatalf("source audit binding = context=%q target=%q", records[0].Context.Name, records[0].Target.Resource)
	}
	if records[2].Context.Name != "dst" || records[2].Target.Resource != "payments" {
		t.Fatalf("target audit binding = context=%q target=%q", records[2].Context.Name, records[2].Target.Resource)
	}
	if records[0].Metadata.PayloadFingerprint == "" || records[2].Metadata.PayloadFingerprint == "" ||
		records[0].Metadata.PayloadFingerprint == records[2].Metadata.PayloadFingerprint {
		t.Fatalf("mirror audit fingerprints are not independently domain-bound: source=%q target=%q", records[0].Metadata.PayloadFingerprint, records[2].Metadata.PayloadFingerprint)
	}

	spec := mirrorTargetMutationAuditSpec("dst", mqgovctx.Context{Backend: "fake"}, request, request.Limit)
	if spec.ContextName != "dst" || spec.Target.Resource != "payments" || spec.Metadata.PayloadFingerprint == "" {
		t.Fatalf("target mutation audit spec is not destination-bound: %+v", spec)
	}
}

func TestMirrorSourceIntentFailurePreventsBackendConstruction(t *testing.T) {
	sourceBuilds := 0
	flags := newDefaultFlags()
	flags.mirrorRuntime = &mirrorCommandRuntime{
		buildSource: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			sourceBuilds++
			return newMirrorSpyBroker("source", nil), nil
		},
	}
	flags.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			return errors.New("injected source intent failure")
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 16)),
	}

	output, err := executeReadAuditTestCommand(t, flags,
		"-o", "json",
		"--yes",
		"message", "mirror", "orders",
		"--to-context", "dst",
		"--to-topic", "orders",
		"--limit", "1",
	)

	if output != "" {
		t.Fatalf("mirror output = %q, want none", output)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeLocalIOError {
		t.Fatalf("mirror error = %v, code = %s, want LOCAL_IO_ERROR", err, code)
	}
	if sourceBuilds != 0 {
		t.Fatalf("source backend builds = %d, want 0 before durable intent", sourceBuilds)
	}
}

func TestMirrorSourceOutcomeFailurePreventsTargetMutationAndPreservesCause(t *testing.T) {
	operationErr := apperrors.New(apperrors.CodeBackendError, "injected source read failure", nil)
	events := make([]string, 0)
	source := newMirrorSpyBroker("source", &events)
	source.messages = []mqgov.Message{{
		Coordinate: mqgov.MessageCoordinate{TopicCoordinate: mqgov.TopicCoordinate{Topic: "orders"}},
		Key:        []byte("source-key-secret"),
		Body:       []byte("source-body-secret"),
		Headers:    map[string][]byte{"authorization": []byte("source-header-secret")},
	}}
	source.mirrorErr = operationErr
	target := newMirrorSpyBroker("target", &events)
	flags := mirrorTestFlags(source, target)
	originalBuildTarget := flags.mirrorRuntime.buildTarget
	flags.mirrorRuntime.buildTarget = func(
		parent *cliFlags,
		meta mqgovctx.Context,
		targetFlags *cliFlags,
		name string,
	) (mqgov.Broker, error) {
		backend, err := originalBuildTarget(parent, meta, targetFlags, name)
		if err == nil {
			readTLSNotify(targetFlags)(tlspin.Event{Address: "target.example:9093", Algorithm: tlspin.Algorithm, Fingerprint: "SHA256:test"})
		}
		return backend, err
	}
	diagnosticFlushes := 0
	flags.readDiagnosticWrite = func(data []byte) (int, error) {
		diagnosticFlushes++
		return len(data), nil
	}
	appendCalls := 0
	flags.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			appendCalls++
			events = append(events, "audit:"+record.Action+":"+record.Phase)
			if record.Kind == readAuditKind && record.Phase == mutationAuditPhaseOutcome {
				return errors.New("injected source outcome failure")
			}
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(appendCalls)).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x22}, 32)),
	}

	output, err := executeReadAuditTestCommand(t, flags,
		"-o", "json",
		"--yes",
		"message", "mirror", "orders",
		"--to-context", "dst",
		"--to-topic", "orders",
		"--limit", "1",
	)

	if output != "" {
		t.Fatalf("mirror output = %q, want none", output)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeLocalIOError {
		t.Fatalf("mirror error = %v, code = %s, want LOCAL_IO_ERROR", err, code)
	}
	if !errors.Is(err, operationErr) {
		t.Fatalf("mirror error = %v, want source backend cause", err)
	}
	if target.produceCalls != 0 || containsMirrorMutationIntent(events) {
		t.Fatalf("target mutation started after source outcome failure: calls=%d events=%v", target.produceCalls, events)
	}
	if diagnosticFlushes != 0 {
		t.Fatalf("target diagnostics flushed %d time(s) after source outcome persistence failed", diagnosticFlushes)
	}
}

func TestMirrorPersistsSourceOutcomeBeforeTargetMutation(t *testing.T) {
	const (
		keySecret    = "source-key-secret"
		bodySecret   = "source-body-secret"
		headerSecret = "source-header-secret"
	)
	events := make([]string, 0)
	source := newMirrorSpyBroker("source", &events)
	source.messages = []mqgov.Message{{
		Coordinate: mqgov.MessageCoordinate{TopicCoordinate: mqgov.TopicCoordinate{Topic: "orders"}},
		Key:        []byte(keySecret),
		Body:       []byte(bodySecret),
		Headers:    map[string][]byte{"authorization": []byte(headerSecret)},
	}}
	target := newMirrorSpyBroker("target", &events)
	flags := mirrorTestFlags(source, target)
	originalBuildTarget := flags.mirrorRuntime.buildTarget
	flags.mirrorRuntime.buildTarget = func(
		parent *cliFlags,
		meta mqgovctx.Context,
		targetFlags *cliFlags,
		name string,
	) (mqgov.Broker, error) {
		backend, err := originalBuildTarget(parent, meta, targetFlags, name)
		if err == nil {
			readTLSNotify(targetFlags)(tlspin.Event{Address: "target.example:9093", Algorithm: tlspin.Algorithm, Fingerprint: "SHA256:test"})
		}
		return backend, err
	}
	flags.readDiagnosticWrite = func(data []byte) (int, error) {
		if !strings.Contains(string(data), "TOFU: pinned TLS certificate") {
			t.Fatalf("unexpected deferred diagnostic: %q", data)
		}
		events = append(events, "diagnostic:tls")
		return len(data), nil
	}
	var records []safeAuditRecord
	flags.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			events = append(events, "audit:"+record.Action+":"+record.Phase)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x33}, 64)),
	}

	output, err := executeReadAuditTestCommand(t, flags,
		"-o", "json",
		"--yes",
		"message", "mirror", "orders",
		"--to-context", "dst",
		"--to-topic", "orders",
		"--limit", "1",
	)
	if err != nil {
		t.Fatalf("mirror error = %v; output=%s; events=%v", err, output, events)
	}
	if target.produceCalls != 1 {
		t.Fatalf("target produce calls = %d, want 1", target.produceCalls)
	}
	assertEventOrder(t, events,
		"audit:mq.message.mirror.source:intent",
		"source:build",
		"source:mirror-read",
		"audit:mq.message.mirror.source:outcome",
		"diagnostic:tls",
		"audit:mq.message.mirror:intent",
		"target:produce",
		"audit:mq.message.mirror:outcome",
	)
	serialized, marshalErr := json.Marshal(records)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	for _, secret := range []string{keySecret, bodySecret, headerSecret} {
		if strings.Contains(string(serialized), secret) || strings.Contains(output, secret) {
			t.Fatalf("mirror audit/output leaked %q: records=%s output=%s", secret, serialized, output)
		}
	}
}

func TestMirrorBufferByteLimitFailsBeforeTargetMutation(t *testing.T) {
	events := make([]string, 0)
	source := newMirrorSpyBroker("source", &events)
	source.messages = []mqgov.Message{
		{Body: []byte("12345")},
		{Body: []byte("6789")},
	}
	target := newMirrorSpyBroker("target", &events)
	flags := mirrorTestFlags(source, target)
	flags.mirrorRuntime.maxBufferedBytes = mirrorMessageOverhead + 8
	var records []safeAuditRecord
	flags.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			events = append(events, "audit:"+record.Action+":"+record.Phase)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x44}, 32)),
	}

	output, err := executeReadAuditTestCommand(t, flags,
		"-o", "json",
		"--yes",
		"message", "mirror", "orders",
		"--to-context", "dst",
		"--to-topic", "orders",
		"--limit", "2",
	)

	if output != "" {
		t.Fatalf("mirror output = %q, want none", output)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeValidationFailed {
		t.Fatalf("mirror error = %v, code = %s, want VALIDATION_FAILED", err, code)
	}
	if target.produceCalls != 0 || containsMirrorMutationIntent(events) {
		t.Fatalf("target mutation started after source buffer overflow: calls=%d events=%v", target.produceCalls, events)
	}
	if len(records) < 2 {
		t.Fatalf("source overflow audit records = %+v", records)
	}
	outcome := records[len(records)-1]
	if outcome.Kind != readAuditKind ||
		outcome.Outcome == nil ||
		outcome.Outcome.Status != audit.StatusFailed ||
		outcome.Outcome.ErrorCode != string(apperrors.CodeValidationFailed) ||
		outcome.Outcome.ResultCount != 1 {
		t.Fatalf("source overflow audit records = %+v", records)
	}
	serialized, marshalErr := json.Marshal(records)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(serialized), "12345") || strings.Contains(string(serialized), "6789") {
		t.Fatalf("source overflow audit leaked payloads: %s", serialized)
	}
}

func TestMirrorBufferAccountsForEmptyHeaderOverhead(t *testing.T) {
	events := make([]string, 0)
	headers := make(map[string][]byte, 128)
	for index := 0; index < 128; index++ {
		headers[fmt.Sprintf("h%03d", index)] = []byte{}
	}
	source := newMirrorSpyBroker("source", &events)
	source.messages = []mqgov.Message{{Headers: headers}}
	target := newMirrorSpyBroker("target", &events)
	flags := mirrorTestFlags(source, target)
	flags.mirrorRuntime.maxBufferedBytes = 4 * 1024
	var records []safeAuditRecord
	flags.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			events = append(events, "audit:"+record.Action+":"+record.Phase)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x46}, 32)),
	}

	output, err := executeReadAuditTestCommand(t, flags,
		"-o", "json",
		"--yes",
		"message", "mirror", "orders",
		"--to-context", "dst",
		"--to-topic", "orders",
		"--limit", "1",
	)

	if output != "" {
		t.Fatalf("mirror output = %q, want none", output)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeValidationFailed {
		t.Fatalf("mirror error = %v, code = %s, want VALIDATION_FAILED", err, code)
	}
	if target.produceCalls != 0 || containsMirrorMutationIntent(events) {
		t.Fatalf("target mutation started after header accounting overflow: calls=%d events=%v", target.produceCalls, events)
	}
	if len(records) < 2 {
		t.Fatalf("source header-overflow audit records = %+v", records)
	}
	outcome := records[len(records)-1]
	if outcome.Outcome == nil ||
		outcome.Outcome.Status != audit.StatusFailed ||
		outcome.Outcome.ResultCount != 0 {
		t.Fatalf("source header-overflow audit records = %+v", records)
	}
}

func TestMirrorSourceCannotEmitPastRequestedLimit(t *testing.T) {
	events := make([]string, 0)
	source := newMirrorSpyBroker("source", &events)
	source.messages = []mqgov.Message{
		{Body: []byte("first")},
		{Body: []byte("second")},
	}
	target := newMirrorSpyBroker("target", &events)
	flags := mirrorTestFlags(source, target)
	var records []safeAuditRecord
	flags.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			events = append(events, "audit:"+record.Action+":"+record.Phase)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x45}, 32)),
	}

	output, err := executeReadAuditTestCommand(t, flags,
		"-o", "json",
		"--yes",
		"message", "mirror", "orders",
		"--to-context", "dst",
		"--to-topic", "orders",
		"--limit", "1",
	)

	if output != "" {
		t.Fatalf("mirror output = %q, want none", output)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeValidationFailed {
		t.Fatalf("mirror error = %v, code = %s, want VALIDATION_FAILED", err, code)
	}
	if target.produceCalls != 0 || containsMirrorMutationIntent(events) {
		t.Fatalf("target mutation started after source exceeded request limit: calls=%d events=%v", target.produceCalls, events)
	}
	if len(records) < 2 {
		t.Fatalf("source over-emission audit records = %+v", records)
	}
	outcome := records[len(records)-1]
	if outcome.Kind != readAuditKind ||
		outcome.Outcome == nil ||
		outcome.Outcome.Status != audit.StatusFailed ||
		outcome.Outcome.ErrorCode != string(apperrors.CodeValidationFailed) ||
		outcome.Outcome.ResultCount != 1 {
		t.Fatalf("source over-emission audit records = %+v", records)
	}
}

func TestMirrorAndTailRejectUnboundedAllocationRequests(t *testing.T) {
	sourceBuilds := 0
	flags := newDefaultFlags()
	flags.mirrorRuntime = &mirrorCommandRuntime{
		buildSource: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			sourceBuilds++
			return newMirrorSpyBroker("source", nil), nil
		},
	}
	output, err := executeReadAuditTestCommand(t, flags,
		"message", "mirror", "orders",
		"--to-context", "dst",
		"--to-topic", "orders",
		"--limit", "1001",
	)
	if output != "" {
		t.Fatalf("oversized mirror output = %q, want none", output)
	}
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeUsageError {
		t.Fatalf("oversized mirror error = %v, code = %s, want USAGE_ERROR", err, code)
	}
	if sourceBuilds != 0 {
		t.Fatalf("source backend builds = %d, want 0 for oversized mirror request", sourceBuilds)
	}

	if err := validateTailWindow(int(^uint(0) >> 1)); apperrors.AsAppError(err).Code != apperrors.CodeUsageError {
		t.Fatalf("unbounded tail validation error = %v, want USAGE_ERROR", err)
	}
	if capacity := tailBufferCapacity(maxTailMessages); capacity != tailBufferInitialLimit {
		t.Fatalf("tail initial buffer capacity = %d, want %d", capacity, tailBufferInitialLimit)
	}
}

func TestPeekCommandsRejectOversizedCountsBeforeBackendConstruction(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "message", args: []string{"message", "peek", "orders", "--count", "10001"}},
		{name: "dlq", args: []string{"dlq", "peek", "orders.dlq", "--count", "10001"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			buildCalls := 0
			flags := newDefaultFlags()
			flags.brokerRuntime = &brokerCommandRuntime{
				buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
					buildCalls++
					return newMirrorSpyBroker("backend", nil), nil
				},
			}

			output, err := executeReadAuditTestCommand(t, flags, test.args...)

			if output != "" {
				t.Fatalf("oversized peek output = %q, want none", output)
			}
			if code := apperrors.AsAppError(err).Code; code != apperrors.CodeUsageError {
				t.Fatalf("oversized peek error = %v, code = %s, want USAGE_ERROR", err, code)
			}
			if buildCalls != 0 {
				t.Fatalf("backend build calls = %d, want 0", buildCalls)
			}
		})
	}
}

func TestProduceMirroredMessagesReportsTimeoutAsUncertainPartialFailure(t *testing.T) {
	target := newMirrorSpyBroker("target", nil)
	target.produceErrAt = 2
	target.produceErr = apperrors.New(apperrors.CodeBackendError, "produce timed out", context.DeadlineExceeded)
	messages := []mqgov.Message{{Body: []byte("first")}, {Body: []byte("second")}, {Body: []byte("third")}}

	outcome, err := produceMirroredMessages(t.Context(), target, mqgov.TopicCoordinate{Topic: "orders"}, messages)
	if outcome != (mqgov.BatchOutcome{Succeeded: 1, Uncertain: 1}) {
		t.Fatalf("mirror outcome = %+v, want one success and one uncertain produce", outcome)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodePartialFailure {
		t.Fatalf("mirror error code = %s, want %s; err=%v", got, apperrors.CodePartialFailure, err)
	}
	if target.produceCalls != 2 {
		t.Fatalf("produce calls = %d, want stop after uncertain second produce", target.produceCalls)
	}
}

func TestProduceMirroredMessagesReportsPriorSuccessAsPartialFailure(t *testing.T) {
	target := newMirrorSpyBroker("target", nil)
	target.produceErrAt = 2
	target.produceErr = apperrors.New(apperrors.CodeAuthFailed, "produce rejected", nil)
	messages := []mqgov.Message{{Body: []byte("first")}, {Body: []byte("second")}}

	outcome, err := produceMirroredMessages(t.Context(), target, mqgov.TopicCoordinate{Topic: "orders"}, messages)
	if outcome != (mqgov.BatchOutcome{Succeeded: 1, Failed: 1}) {
		t.Fatalf("mirror outcome = %+v, want one success and one definitive failure", outcome)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodePartialFailure {
		t.Fatalf("mirror error code = %s, want %s; err=%v", got, apperrors.CodePartialFailure, err)
	}
}

func TestProduceMirroredMessagesKeepsFirstDefinitiveFailure(t *testing.T) {
	target := newMirrorSpyBroker("target", nil)
	target.produceErrAt = 1
	target.produceErr = apperrors.New(apperrors.CodeAuthFailed, "produce rejected", nil)

	outcome, err := produceMirroredMessages(t.Context(), target, mqgov.TopicCoordinate{Topic: "orders"}, []mqgov.Message{{Body: []byte("first")}})
	if outcome != (mqgov.BatchOutcome{Failed: 1}) {
		t.Fatalf("mirror outcome = %+v, want one definitive failure", outcome)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeAuthFailed {
		t.Fatalf("mirror error code = %s, want %s; err=%v", got, apperrors.CodeAuthFailed, err)
	}
}

type mirrorSpyBroker struct {
	mqgov.Broker
	label        string
	events       *[]string
	messages     []mqgov.Message
	mirrorErr    error
	produceErr   error
	produceErrAt int
	produceCalls int
}

func newMirrorSpyBroker(label string, events *[]string) *mirrorSpyBroker {
	return &mirrorSpyBroker{
		Broker: fake.New("fake", ""),
		label:  label,
		events: events,
	}
}

func (broker *mirrorSpyBroker) DescribeTopic(ctx context.Context, coordinate mqgov.TopicCoordinate) (mqgov.TopicDescription, error) {
	broker.addEvent("describe")
	return broker.Broker.DescribeTopic(ctx, coordinate)
}

func (broker *mirrorSpyBroker) MirrorMessages(
	_ context.Context,
	request mqgov.MessageMirrorRequest,
	emit func(mqgov.Message) error,
) (mqgov.MessageMirrorResult, error) {
	broker.addEvent("mirror-read")
	result := mqgov.MessageMirrorResult{
		Source: request.Source,
		Target: request.Target,
		DryRun: request.DryRun,
	}
	for _, message := range broker.messages {
		if err := emit(message); err != nil {
			return result, err
		}
		result.Count++
	}
	return result, broker.mirrorErr
}

func (broker *mirrorSpyBroker) Produce(_ context.Context, request mqgov.MessageProduceRequest) (mqgov.MessageProduceResult, error) {
	broker.produceCalls++
	broker.addEvent("produce")
	if broker.produceErrAt > 0 && broker.produceCalls == broker.produceErrAt {
		return mqgov.MessageProduceResult{}, broker.produceErr
	}
	return mqgov.MessageProduceResult{
		Coordinate: mqgov.MessageCoordinate{TopicCoordinate: request.Coordinate},
		Fingerprint: mqgov.Fingerprints(
			request.Key,
			request.Body,
			1,
		),
	}, nil
}

func (broker *mirrorSpyBroker) addEvent(action string) {
	if broker.events != nil {
		*broker.events = append(*broker.events, broker.label+":"+action)
	}
}

func mirrorTestFlags(source, target mqgov.Broker) *cliFlags {
	flags := newDefaultFlags()
	flags.mirrorRuntime = &mirrorCommandRuntime{
		buildSource: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			if spy, ok := source.(*mirrorSpyBroker); ok {
				spy.addEvent("build")
			}
			return source, nil
		},
		resolveTarget: func(f *cliFlags, name string) (mqgovctx.Context, *cliFlags, error) {
			local := fleetLocalFlags(f, name)
			return mqgovctx.Context{Backend: "fake"}, &local, nil
		},
		buildTarget: func(_ *cliFlags, _ mqgovctx.Context, _ *cliFlags, _ string) (mqgov.Broker, error) {
			if spy, ok := target.(*mirrorSpyBroker); ok {
				spy.addEvent("build")
			}
			return target, nil
		},
	}
	return flags
}

func containsMirrorMutationIntent(events []string) bool {
	for _, event := range events {
		if event == "audit:mq.message.mirror:intent" {
			return true
		}
	}
	return false
}

func assertEventOrder(t *testing.T, events []string, expected ...string) {
	t.Helper()
	last := -1
	for _, want := range expected {
		found := -1
		for index := last + 1; index < len(events); index++ {
			if events[index] == want {
				found = index
				break
			}
		}
		if found < 0 {
			t.Fatalf("event %q not found after index %d in %v", want, last, events)
		}
		last = found
	}
}
