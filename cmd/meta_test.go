package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestVersionPlain(t *testing.T) {
	SetVersionInfo("v0.0.0-test", "deadbeef", "2026-06-29")
	t.Cleanup(func() { SetVersionInfo("dev", "", "") })

	out, err := runCommandForTest(t, "-o", "plain", "version")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if want := "v0.0.0-test\n"; out != want {
		t.Fatalf("unexpected version plain: %q", out)
	}
}

func TestCapabilitiesPlain(t *testing.T) {
	out, err := runCommandForTest(t, "-o", "plain", "capabilities")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := strings.Join(capabilityPlainCommands(), "\n") + "\n"
	if out != want {
		t.Fatalf("unexpected capabilities plain:\n%s", out)
	}
	if strings.Contains(out, "{") || strings.Contains(out, "\t") {
		t.Fatalf("capabilities plain should be a command list, got %q", out)
	}
}

func TestCapabilitiesJSONFamilySchema(t *testing.T) {
	data := buildCapabilities(mqgov.Capabilities{
		Backend:       "fake",
		ResourceTypes: []string{"topic"},
		Verbs:         []string{"list"},
	})
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal(capabilities) error = %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil {
		t.Fatalf("capabilities output is not JSON: %v\n%s", err, string(payload))
	}
	var env struct {
		Supported struct {
			ContextAPIVersions []string        `json:"contextApiVersions"`
			AuditAPIVersions   []string        `json:"auditApiVersions"`
			ReadAudit          string          `json:"readAudit"`
			ReadAuditScope     string          `json:"readAuditScope"`
			ReadLimits         capReadLimits   `json:"readLimits"`
			Commands           json.RawMessage `json:"commands"`
		} `json:"supported"`
		Domain struct {
			Backend       json.RawMessage `json:"backend"`
			OutputFormats []string        `json:"outputFormats"`
			ErrorCodes    []string        `json:"errorCodes"`
			ExitCodes     []int           `json:"exitCodes"`
		} `json:"domain"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		t.Fatalf("Unmarshal(capabilities) error = %v\n%s", err, string(payload))
	}
	if strings.Join(env.Supported.ContextAPIVersions, ",") != "mqgov-cli.io/context/v1" {
		t.Fatalf("context API versions = %#v", env.Supported.ContextAPIVersions)
	}
	if strings.Join(env.Supported.AuditAPIVersions, ",") != auditAPIVersion {
		t.Fatalf("audit API versions = %#v", env.Supported.AuditAPIVersions)
	}
	if env.Supported.ReadAudit != "required-intent-outcome" {
		t.Fatalf("read audit = %q, want required-intent-outcome", env.Supported.ReadAudit)
	}
	if env.Supported.ReadAuditScope != "all-backend-reads-and-mutation-preflights" ||
		env.Supported.ReadLimits.PeekMessages != maxPeekMessages ||
		env.Supported.ReadLimits.TailMessages != maxTailMessages ||
		env.Supported.ReadLimits.MirrorMessages != maxMirrorMessages ||
		env.Supported.ReadLimits.MirrorBufferAccountingBytes != maxMirrorBufferedBytes {
		t.Fatalf("read audit scope/limits = %q %+v", env.Supported.ReadAuditScope, env.Supported.ReadLimits)
	}
	if len(env.Supported.Commands) != 0 || top["backend"] != nil {
		t.Fatalf("domain fields leaked outside domain: %s", string(payload))
	}
	if len(env.Domain.Backend) == 0 {
		t.Fatalf("domain missing backend: %+v", env.Domain)
	}
	if strings.Join(env.Domain.OutputFormats, ",") != "table,json,plain" || len(env.Domain.ErrorCodes) == 0 || len(env.Domain.ExitCodes) == 0 {
		t.Fatalf("domain metadata incomplete: %+v", env.Domain)
	}
	foundAuditPrune := false
	for _, command := range data.Domain.Commands {
		if command.Noun == "audit" && command.Verb == "prune" {
			foundAuditPrune = command.Risk == "R3" && command.AllowFlag == "allow-audit-prune"
		}
	}
	if !foundAuditPrune {
		t.Fatalf("capabilities missing governed audit prune: %+v", data.Domain.Commands)
	}
}

func TestCapabilitiesReportsBackendSpecificTopicGovernance(t *testing.T) {
	t.Parallel()
	tests := []struct {
		backend     string
		createRisk  string
		createAllow string
		deleteRisk  string
		deleteAllow string
	}{
		{backend: "kafka", createRisk: "R1/R2 protected", deleteRisk: "R3", deleteAllow: "allow-topic-delete"},
		{backend: "rocketmq", createRisk: "R2/R3 protected", createAllow: "allow-topic-upsert", deleteRisk: "NotImplemented"},
	}
	for _, test := range tests {
		t.Run(test.backend, func(t *testing.T) {
			t.Parallel()
			data := buildCapabilities(mqgov.Capabilities{Backend: test.backend})
			var create, deleteCommand capCommand
			for _, command := range data.Domain.Commands {
				if command.Noun != "topic" {
					continue
				}
				switch command.Verb {
				case "create":
					create = command
				case "delete":
					deleteCommand = command
				}
			}
			if create.Risk != test.createRisk || create.AllowFlag != test.createAllow {
				t.Fatalf("topic create capability = %+v, want risk=%q allow=%q", create, test.createRisk, test.createAllow)
			}
			if deleteCommand.Risk != test.deleteRisk || deleteCommand.AllowFlag != test.deleteAllow {
				t.Fatalf("topic delete capability = %+v, want risk=%q allow=%q", deleteCommand, test.deleteRisk, test.deleteAllow)
			}
		})
	}
}

func TestGlobalFlagsHelp(t *testing.T) {
	out, err := runCommandForTest(t, "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, flag := range []string{"--debug", "--trace", "--no-color", "--allow-topic-upsert", "--allow-audit-prune"} {
		if !strings.Contains(out, flag) {
			t.Fatalf("help missing %s:\n%s", flag, out)
		}
	}
}

func TestGlobalFlagsWithVersion(t *testing.T) {
	SetVersionInfo("v0.0.0-test", "deadbeef", "2026-06-29")
	t.Cleanup(func() { SetVersionInfo("dev", "", "") })

	out, err := runCommandForTest(t, "--debug", "--trace", "--no-color", "-o", "plain", "version")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if want := "v0.0.0-test\n"; out != want {
		t.Fatalf("version plain = %q, want %q", out, want)
	}
}
