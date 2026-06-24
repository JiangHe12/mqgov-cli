package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/apperrors"

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
	if got["bad"].Status != "unreachable" || !strings.Contains(got["bad"].Error, "NOT_IMPLEMENTED") {
		t.Fatalf("bad status = %+v, want unreachable NotImplemented", got["bad"])
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
	if !strings.Contains(text, "mq.fleet") || !strings.Contains(text, "dev-a") || !strings.Contains(text, "count=") {
		t.Fatalf("audit log missing fleet summary: %s", text)
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
