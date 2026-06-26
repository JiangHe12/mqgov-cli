package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestOperationTargetTableHeader(t *testing.T) {
	out, err := runCommandForTest(t, "topic", "list")
	if err != nil {
		t.Fatalf("topic list error = %v; out=%s", err, out)
	}
	want := "TARGET\tcontext=direct | backend=fake | cluster=fake | namespace=\n\n"
	if !strings.HasPrefix(out, want) {
		t.Fatalf("output prefix = %q, want %q; full output=%s", out[:min(len(out), len(want))], want, out)
	}
}

func TestOperationTargetJSONListEnvelope(t *testing.T) {
	out, err := runCommandForTest(t, "-o", "json", "topic", "list")
	if err != nil {
		t.Fatalf("topic list error = %v; out=%s", err, out)
	}
	var payload struct {
		Kind string `json:"kind"`
		Data struct {
			Target operationTarget `json:"target"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; out=%s", err, out)
	}
	want := operationTarget{Context: "direct", Backend: "fake", Cluster: "fake"}
	if payload.Kind != "TopicList" || payload.Data.Target != want {
		t.Fatalf("payload target = %+v kind=%s, want %+v TopicList", payload.Data.Target, payload.Kind, want)
	}
}

func TestOperationTargetDoesNotExposeRabbitMQURLs(t *testing.T) {
	target := operationTargetFromDescription("prod", mqgov.Description{Backend: "rabbitmq", Cluster: "rabbitmq", Namespace: "/"})
	data, err := json.Marshal(target)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	text := string(data)
	for _, forbidden := range []string{"guest:secret", "amqp://", "http://guest:secret@"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("target leaked credential-bearing URL fragment %q: %s", forbidden, text)
		}
	}
}
