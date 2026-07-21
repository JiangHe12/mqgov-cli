//go:build integration

package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kafkabackend "github.com/JiangHe12/mqgov-cli/internal/backend/kafka"
	pulsarbackend "github.com/JiangHe12/mqgov-cli/internal/backend/pulsar"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func TestMessageMirrorCrossBrokerIntegration(t *testing.T) {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	pulsarServiceURL := os.Getenv("PULSAR_SERVICE_URL")
	pulsarAdminURL := os.Getenv("PULSAR_ADMIN_URL")
	if strings.TrimSpace(kafkaBrokers) == "" || strings.TrimSpace(pulsarServiceURL) == "" || strings.TrimSpace(pulsarAdminURL) == "" {
		skipOrFailIntegration(t, "KAFKA_BROKERS, PULSAR_SERVICE_URL and PULSAR_ADMIN_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	kafkaBroker, err := kafkabackend.New(kafkabackend.Options{Brokers: []string{kafkaBrokers}, Cluster: "integration", Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("kafka New() error = %v", err)
	}
	pulsarBroker, err := pulsarbackend.New(pulsarbackend.Options{
		ServiceURL: pulsarServiceURL,
		AdminURL:   pulsarAdminURL,
		Tenant:     getenvDefault("PULSAR_TENANT", "public"),
		Namespace:  getenvDefault("PULSAR_NAMESPACE", "default"),
		Cluster:    "integration",
		Timeout:    15 * time.Second,
	})
	if err != nil {
		t.Fatalf("pulsar New() error = %v", err)
	}

	runKafkaToPulsarMirror(t, ctx, kafkaBroker, pulsarBroker, kafkaBrokers, pulsarServiceURL, pulsarAdminURL)
	runPulsarToKafkaMirror(t, ctx, kafkaBroker, pulsarBroker, kafkaBrokers, pulsarServiceURL, pulsarAdminURL)
}

func skipOrFailIntegration(t *testing.T, reason string) {
	t.Helper()
	if strings.EqualFold(strings.TrimSpace(os.Getenv("MQGOV_INTEGRATION_REQUIRED")), "true") {
		t.Fatalf("%s while MQGOV_INTEGRATION_REQUIRED=true", reason)
	}
	t.Skip(reason)
}

func runKafkaToPulsarMirror(t *testing.T, ctx context.Context, kafkaBroker *kafkabackend.Broker, pulsarBroker *pulsarbackend.Broker, kafkaBrokers, pulsarServiceURL, pulsarAdminURL string) {
	t.Helper()
	sourceTopic := fmt.Sprintf("mqgov-it-mirror-k2p-src-%d", time.Now().UnixNano())
	targetTopic := fmt.Sprintf("mqgov-it-mirror-k2p-dst-%d", time.Now().UnixNano())
	sourceCoord := mqgov.TopicCoordinate{Cluster: "integration", Topic: sourceTopic}
	targetCoord := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "public/default", Topic: targetTopic}
	defer func() { _ = kafkaBroker.DeleteTopic(context.Background(), sourceCoord) }()
	defer func() { _ = pulsarBroker.DeleteTopic(context.Background(), targetCoord) }()
	if _, err := kafkaBroker.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: sourceCoord, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(kafka source) error = %v", err)
	}
	if _, err := pulsarBroker.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: targetCoord, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(pulsar target) error = %v", err)
	}
	bodies := [][]byte{[]byte("kafka-to-pulsar-a"), []byte("kafka-to-pulsar-b")}
	for i, body := range bodies {
		if _, err := kafkaBroker.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: sourceCoord, Key: []byte(fmt.Sprintf("k%d", i)), Body: body, Headers: map[string][]byte{"x-mirror": []byte("k2p")}}); err != nil {
			t.Fatalf("Produce(kafka source) error = %v", err)
		}
	}
	runMirrorCommand(t, kafkaContext(kafkaBrokers), pulsarContext(pulsarServiceURL, pulsarAdminURL), sourceTopic, targetTopic, len(bodies))
	got := collectMirroredBodies(t, ctx, pulsarBroker, targetCoord, len(bodies))
	assertBodyHashes(t, got, bodies)
}

func runPulsarToKafkaMirror(t *testing.T, ctx context.Context, kafkaBroker *kafkabackend.Broker, pulsarBroker *pulsarbackend.Broker, kafkaBrokers, pulsarServiceURL, pulsarAdminURL string) {
	t.Helper()
	sourceTopic := fmt.Sprintf("mqgov-it-mirror-p2k-src-%d", time.Now().UnixNano())
	targetTopic := fmt.Sprintf("mqgov-it-mirror-p2k-dst-%d", time.Now().UnixNano())
	sourceCoord := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "public/default", Topic: sourceTopic}
	targetCoord := mqgov.TopicCoordinate{Cluster: "integration", Topic: targetTopic}
	defer func() { _ = pulsarBroker.DeleteTopic(context.Background(), sourceCoord) }()
	defer func() { _ = kafkaBroker.DeleteTopic(context.Background(), targetCoord) }()
	if _, err := pulsarBroker.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: sourceCoord, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(pulsar source) error = %v", err)
	}
	if _, err := kafkaBroker.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: targetCoord, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(kafka target) error = %v", err)
	}
	bodies := [][]byte{[]byte("pulsar-to-kafka-a"), []byte("pulsar-to-kafka-b")}
	for i, body := range bodies {
		if _, err := pulsarBroker.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: sourceCoord, Key: []byte(fmt.Sprintf("p%d", i)), Body: body, Headers: map[string][]byte{"x-mirror": []byte("p2k")}}); err != nil {
			t.Fatalf("Produce(pulsar source) error = %v", err)
		}
	}
	runMirrorCommand(t, pulsarContext(pulsarServiceURL, pulsarAdminURL), kafkaContext(kafkaBrokers), sourceTopic, targetTopic, len(bodies))
	got := collectMirroredBodies(t, ctx, kafkaBroker, targetCoord, len(bodies))
	assertBodyHashes(t, got, bodies)
}

func runMirrorCommand(t *testing.T, source, target mqgovctx.Context, sourceTopic, targetTopic string, limit int) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	if err := mqgovctx.Set("src", source); err != nil {
		t.Fatalf("set source context: %v", err)
	}
	if err := mqgovctx.Set("dst", target); err != nil {
		t.Fatalf("set target context: %v", err)
	}
	if err := mqgovctx.Use("src"); err != nil {
		t.Fatalf("use source context: %v", err)
	}
	out, err := runCommandForTest(t, "-o", "json", "--yes", "message", "mirror", sourceTopic, "--to-context", "dst", "--to-topic", targetTopic, "--limit", fmt.Sprintf("%d", limit))
	if err != nil {
		t.Fatalf("message mirror error = %v; out=%s", err, out)
	}
}

func collectMirroredBodies(t *testing.T, ctx context.Context, source mqgov.MirrorSource, coord mqgov.TopicCoordinate, limit int) [][]byte {
	t.Helper()
	out := make([][]byte, 0, limit)
	_, err := source.MirrorMessages(ctx, mqgov.MessageMirrorRequest{Source: coord, Target: coord, From: "earliest", Partition: -1, Limit: limit, DryRun: true}, func(msg mqgov.Message) error {
		out = append(out, append([]byte(nil), msg.Body...))
		return nil
	})
	if err != nil {
		t.Fatalf("collect mirrored bodies error = %v", err)
	}
	if len(out) != limit {
		t.Fatalf("mirrored body count = %d, want %d", len(out), limit)
	}
	return out
}

func assertBodyHashes(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("body count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if mqgov.SHA256Hex(got[i]) != mqgov.SHA256Hex(want[i]) {
			t.Fatalf("body[%d] sha256 = %s, want %s", i, mqgov.SHA256Hex(got[i]), mqgov.SHA256Hex(want[i]))
		}
	}
}

func kafkaContext(brokers string) mqgovctx.Context {
	return mqgovctx.Context{Backend: "kafka", Cluster: "integration", KafkaBrokers: []string{brokers}}
}

func pulsarContext(serviceURL, adminURL string) mqgovctx.Context {
	return mqgovctx.Context{Backend: "pulsar", Cluster: "integration", PulsarServiceURL: serviceURL, PulsarAdminURL: adminURL, PulsarTenant: getenvDefault("PULSAR_TENANT", "public"), PulsarNamespace: getenvDefault("PULSAR_NAMESPACE", "default")}
}

func getenvDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
