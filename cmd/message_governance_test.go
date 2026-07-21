package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/audit"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
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

	appendMirrorReadAudit(flags, mqgovctx.Context{Backend: "fake"}, "src", request, result, audit.StatusSuccess, nil)
	appendMirrorTargetPreviewAudit(flags, mqgovctx.Context{Backend: "fake"}, "dst", request, result, audit.StatusSuccess, nil)

	path, err := audit.DefaultPath()
	if err != nil {
		t.Fatal(err)
	}
	query, err := audit.QueryRaw(path, audit.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Records) != 2 {
		t.Fatalf("audit record count = %d, want 2", len(query.Records))
	}
	records := make([]safeAuditRecord, 0, 2)
	for _, raw := range query.Records {
		var record safeAuditRecord
		if err := json.Unmarshal([]byte(raw.Line), &record); err != nil {
			t.Fatalf("audit decode error = %v; line=%s", err, raw.Line)
		}
		records = append(records, record)
	}
	if records[0].Context.Name != "src" || records[0].Target.Resource != "orders" {
		t.Fatalf("source audit binding = context=%q target=%q", records[0].Context.Name, records[0].Target.Resource)
	}
	if records[1].Context.Name != "dst" || records[1].Target.Resource != "payments" {
		t.Fatalf("target audit binding = context=%q target=%q", records[1].Context.Name, records[1].Target.Resource)
	}
	if records[0].Metadata.PayloadFingerprint == "" || records[1].Metadata.PayloadFingerprint == "" ||
		records[0].Metadata.PayloadFingerprint == records[1].Metadata.PayloadFingerprint {
		t.Fatalf("mirror audit fingerprints are not independently domain-bound: source=%q target=%q", records[0].Metadata.PayloadFingerprint, records[1].Metadata.PayloadFingerprint)
	}

	spec := mirrorTargetMutationAuditSpec("dst", mqgovctx.Context{Backend: "fake"}, request, request.Limit)
	if spec.ContextName != "dst" || spec.Target.Resource != "payments" || spec.Metadata.PayloadFingerprint == "" {
		t.Fatalf("target mutation audit spec is not destination-bound: %+v", spec)
	}
}
