package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func TestFleetStatusAllAndPartialFailure(t *testing.T) {
	writeFleetTestConfig(t, map[string]mqgovctx.Context{
		"dev-a": {Backend: "fake", Cluster: "cluster-a", Namespace: "ns-a"},
		"bad":   {Backend: "unknown", Cluster: "cluster-b"},
	})

	out, err := runCommandForTest(t, "-o", "json", "fleet", "status", "--all")
	if err != nil {
		t.Fatalf("fleet status error = %v; out=%s", err, out)
	}
	var payload struct {
		Kind string `json:"kind"`
		Data struct {
			Items []fleetStatusItem `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; out=%s", err, out)
	}
	if payload.Kind != "FleetStatus" || len(payload.Data.Items) != 2 {
		t.Fatalf("payload = %+v, want FleetStatus with two contexts", payload)
	}
	got := fleetStatusByContext(payload.Data.Items)
	if got["dev-a"].Status != "success" || got["dev-a"].Backend != "fake" || !got["dev-a"].Capabilities.SupportsOffsets {
		t.Fatalf("dev-a status = %+v, want reachable fake capabilities", got["dev-a"])
	}
	if got["bad"].Status != "error" || !strings.Contains(got["bad"].Error, "NOT_IMPLEMENTED") {
		t.Fatalf("bad status = %+v, want error NotImplemented", got["bad"])
	}
}

func TestFleetTopicsExplicitContexts(t *testing.T) {
	writeFleetTestConfig(t, map[string]mqgovctx.Context{
		"dev-b": {Backend: "fake", Cluster: "cluster-b", Namespace: "ns-b"},
		"dev-a": {Backend: "fake", Cluster: "cluster-a", Namespace: "ns-a"},
	})

	out, err := runCommandForTest(t, "-o", "json", "fleet", "topics", "--contexts", "dev-a,dev-b", "--pattern", "orders")
	if err != nil {
		t.Fatalf("fleet topics error = %v; out=%s", err, out)
	}
	var payload struct {
		Kind string `json:"kind"`
		Data struct {
			Items []fleetTopicItem `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; out=%s", err, out)
	}
	if payload.Kind != "FleetTopics" || len(payload.Data.Items) != 2 {
		t.Fatalf("payload = %+v, want FleetTopics with two contexts", payload)
	}
	for _, item := range payload.Data.Items {
		if item.Status != "success" || item.Count != 1 || len(item.Topics) != 1 || item.Topics[0].Coordinate.Topic != "orders" {
			t.Fatalf("topic item = %+v, want one orders topic", item)
		}
	}
}

func TestFleetUsageErrors(t *testing.T) {
	writeFleetTestConfig(t, map[string]mqgovctx.Context{"dev": {Backend: "fake"}})
	tests := [][]string{
		{"-o", "json", "fleet", "status"},
		{"-o", "json", "fleet", "status", "--all", "--contexts", "dev"},
		{"-o", "json", "fleet", "topics", "--contexts", "missing"},
	}
	for _, args := range tests {
		out, err := runCommandForTest(t, args...)
		if got := apperrors.ExitCode(err); got == 0 {
			t.Fatalf("args %v exit = 0, want usage error; out=%s", args, out)
		}
	}
}

func TestFleetFailureStatuses(t *testing.T) {
	writeFleetTestConfig(t, map[string]mqgovctx.Context{
		"denied":      {Base: corectx.Base{Roles: map[string]string{"bob": "reader"}}, Backend: "fake"},
		"unreachable": {Backend: "kafka", KafkaBrokers: []string{"127.0.0.1:1"}},
		"error":       {Backend: "unknown"},
	})

	out, err := runCommandForTest(t, "-o", "json", "--operator", "alice", "--timeout", "100ms", "fleet", "status", "--contexts", "denied,unreachable")
	if err != nil {
		t.Fatalf("fleet status error = %v; out=%s", err, out)
	}
	statusItems := decodeFleetStatusItems(t, out)
	gotStatus := fleetStatusByContext(statusItems)
	if gotStatus["denied"].Status != "denied" || !strings.Contains(gotStatus["denied"].Error, "AUTHORIZATION_REQUIRED") {
		t.Fatalf("denied status = %+v, want denied authorization error", gotStatus["denied"])
	}
	if gotStatus["unreachable"].Status != "unreachable" || !strings.Contains(gotStatus["unreachable"].Error, "BACKEND_UNREACHABLE") {
		t.Fatalf("unreachable status = %+v, want backend unreachable", gotStatus["unreachable"])
	}

	out, err = runCommandForTest(t, "-o", "json", "fleet", "topics", "--contexts", "error")
	if err != nil {
		t.Fatalf("fleet topics error = %v; out=%s", err, out)
	}
	topicItems := decodeFleetTopicItems(t, out)
	if len(topicItems) != 1 || topicItems[0].Status != "error" || !strings.Contains(topicItems[0].Error, "NOT_IMPLEMENTED") {
		t.Fatalf("topic items = %+v, want error NotImplemented", topicItems)
	}
}

func TestFleetAuditRecordsContextsAndCounts(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	writeFleetTestConfigAt(t, configPath, map[string]mqgovctx.Context{
		"dev-a": {Backend: "fake"},
		"dev-b": {Backend: "fake"},
	})

	if _, err := runCommandForTest(t, "-o", "json", "--config", configPath, "fleet", "topics", "--all"); err != nil {
		t.Fatalf("fleet topics error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(os.Getenv("HOME"), ".mqgov-cli", "audit.log"))
	if err != nil {
		t.Fatalf("ReadFile(audit.log) error = %v", err)
	}
	text := string(data)
	if !strings.Contains(text, `"kind":"ReadAuditRecord"`) ||
		!strings.Contains(text, `"action":"mq.fleet.topics"`) ||
		!strings.Contains(text, `"operationId":`) ||
		!strings.Contains(text, `"payloadFingerprint":"sha256:`) ||
		!strings.Contains(text, `"items":2`) ||
		!strings.Contains(text, `"resultCount":8`) {
		t.Fatalf("audit log missing fleet summary: %s", text)
	}
	if strings.Contains(text, "dev-a") || strings.Contains(text, "dev-b") {
		t.Fatalf("audit log leaked fleet context names: %s", text)
	}
}

func TestFleetIntentFailurePreventsBackendConstruction(t *testing.T) {
	writeFleetTestConfig(t, map[string]mqgovctx.Context{"dev": {Backend: "fake"}})
	buildCalls := 0
	flags := newDefaultFlags()
	flags.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			buildCalls++
			return nil, errors.New("must not build")
		},
	}
	flags.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, _ safeAuditRecord, _ audit.Options) error {
			return errors.New("injected intent append failure")
		},
		now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x61}, 16)),
	}

	_, err := executeReadAuditTestCommand(t, flags, "-o", "json", "fleet", "status", "--contexts", "dev")

	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeLocalIOError {
		t.Fatalf("fleet error = %v, code = %s, want LOCAL_IO_ERROR", err, code)
	}
	if buildCalls != 0 {
		t.Fatalf("backend build calls = %d, want 0 before durable fleet intent", buildCalls)
	}
}

func TestFleetAuthorizationFailurePreventsBackendConstruction(t *testing.T) {
	writeFleetTestConfig(t, map[string]mqgovctx.Context{
		"protected": {Base: corectx.Base{Roles: map[string]string{"bob": "reader"}}, Backend: "fake"},
	})
	buildCalls := 0
	flags := newDefaultFlags()
	flags.brokerRuntime = &brokerCommandRuntime{
		buildResolved: func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error) {
			buildCalls++
			return nil, errors.New("must not build")
		},
	}

	output, err := executeReadAuditTestCommand(t, flags, "-o", "json", "--operator", "alice", "fleet", "status", "--contexts", "protected")
	if err != nil {
		t.Fatalf("fleet partial dashboard error = %v; output=%s", err, output)
	}
	if buildCalls != 0 {
		t.Fatalf("backend build calls = %d, want 0 after fleet authorization failure", buildCalls)
	}
	items := decodeFleetStatusItems(t, output)
	if len(items) != 1 || items[0].Status != "denied" {
		t.Fatalf("fleet items = %+v, want one denied item", items)
	}
}

func writeFleetTestConfig(t *testing.T, contexts map[string]mqgovctx.Context) {
	t.Helper()
	writeFleetTestConfigAt(t, filepath.Join(t.TempDir(), "config.yaml"), contexts)
}

func writeFleetTestConfigAt(t *testing.T, path string, contexts map[string]mqgovctx.Context) {
	t.Helper()
	mqgovctx.SetConfigPath(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, item := range contexts {
		if err := mqgovctx.Set(name, item); err != nil {
			t.Fatal(err)
		}
	}
}

func fleetStatusByContext(items []fleetStatusItem) map[string]fleetStatusItem {
	out := make(map[string]fleetStatusItem, len(items))
	for _, item := range items {
		out[item.Context] = item
	}
	return out
}

func decodeFleetStatusItems(t *testing.T, out string) []fleetStatusItem {
	t.Helper()
	var payload struct {
		Data struct {
			Items []fleetStatusItem `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; out=%s", err, out)
	}
	return payload.Data.Items
}

func decodeFleetTopicItems(t *testing.T, out string) []fleetTopicItem {
	t.Helper()
	var payload struct {
		Data struct {
			Items []fleetTopicItem `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; out=%s", err, out)
	}
	return payload.Data.Items
}
