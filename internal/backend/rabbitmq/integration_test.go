//go:build integration

package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestRabbitMQIntegration(t *testing.T) {
	if strings.TrimSpace(os.Getenv("RABBITMQ_AMQP_URL")) == "" && strings.TrimSpace(os.Getenv("RABBITMQ_HOST")) == "" {
		t.Skip("RABBITMQ_AMQP_URL or RABBITMQ_HOST not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	backend, err := New(Options{
		AMQPURL:        os.Getenv("RABBITMQ_AMQP_URL"),
		ManagementURL:  os.Getenv("RABBITMQ_MANAGEMENT_URL"),
		Host:           getenvDefault("RABBITMQ_HOST", "127.0.0.1"),
		Port:           envInt("RABBITMQ_PORT"),
		VHost:          os.Getenv("RABBITMQ_VHOST"),
		Cluster:        "integration",
		Username:       getenvDefault("RABBITMQ_USERNAME", "guest"),
		Password:       getenvDefault("RABBITMQ_PASSWORD", "guest"),
		TLS:            os.Getenv("RABBITMQ_TLS") == "true",
		CACertFile:     os.Getenv("RABBITMQ_CA_CERT_FILE"),
		ClientCertFile: os.Getenv("RABBITMQ_CLIENT_CERT_FILE"),
		ClientKeyFile:  os.Getenv("RABBITMQ_CLIENT_KEY_FILE"),
		Timeout:        10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	queue := fmt.Sprintf("mqgov_it_%d", time.Now().UnixNano())
	coord := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "/", Topic: queue}
	defer func() { _ = backend.DeleteTopic(context.Background(), coord) }()

	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: coord}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	items, err := backend.ListTopics(ctx, mqgov.TopicListOptions{Pattern: queue})
	if err != nil {
		t.Fatalf("ListTopics() error = %v", err)
	}
	if len(items) != 1 || items[0].Coordinate.Topic != queue {
		t.Fatalf("ListTopics() = %+v, want queue %q", items, queue)
	}
	desc, err := backend.DescribeTopic(ctx, coord)
	if err != nil {
		t.Fatalf("DescribeTopic() error = %v", err)
	}
	if got := desc.Config["messages"]; got != "0" {
		t.Fatalf("messages before produce = %q, want 0", got)
	}

	produced, err := backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: coord, Body: []byte("body")})
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	desc, err = backend.DescribeTopic(ctx, coord)
	if err != nil {
		t.Fatalf("DescribeTopic() after produce error = %v", err)
	}
	beforePeek := messageCount(t, desc)
	if beforePeek != 1 {
		t.Fatalf("messages after produce = %d, want 1", beforePeek)
	}
	peeked, err := backend.Peek(ctx, mqgov.MessagePeekRequest{Coordinate: coord, Count: 1})
	if err != nil {
		t.Fatalf("Peek() error = %v", err)
	}
	if peeked.Count != 1 || len(peeked.Messages) != 1 {
		t.Fatalf("Peek() = %+v, want one fingerprint", peeked)
	}
	if peeked.Messages[0].BodySHA256 != produced.Fingerprint.BodySHA256 {
		t.Fatalf("peek body fingerprint = %s, want %s", peeked.Messages[0].BodySHA256, produced.Fingerprint.BodySHA256)
	}
	desc, err = backend.DescribeTopic(ctx, coord)
	if err != nil {
		t.Fatalf("DescribeTopic() after peek error = %v", err)
	}
	afterPeek := messageCount(t, desc)
	if afterPeek != beforePeek {
		t.Fatalf("peek changed message count: before=%d after=%d", beforePeek, afterPeek)
	}
	if _, ok := mqgov.SupportsTail(backend); ok {
		t.Fatalf("SupportsTail() = true, want false")
	}

	plan, err := backend.PurgeTopic(ctx, mqgov.TopicPurgeRequest{Coordinate: coord, DryRun: true})
	if err != nil {
		t.Fatalf("PurgeTopic dry-run error = %v", err)
	}
	if plan.Total != int64(beforePeek) {
		t.Fatalf("dry-run purge total = %d, want %d", plan.Total, beforePeek)
	}
	purged, err := backend.PurgeTopic(ctx, mqgov.TopicPurgeRequest{Coordinate: coord})
	if err != nil {
		t.Fatalf("PurgeTopic() error = %v", err)
	}
	if purged.Total != int64(beforePeek) {
		t.Fatalf("purged total = %d, want %d", purged.Total, beforePeek)
	}
	desc, err = backend.DescribeTopic(ctx, coord)
	if err != nil {
		t.Fatalf("DescribeTopic() after purge error = %v", err)
	}
	if got := messageCount(t, desc); got != 0 {
		t.Fatalf("messages after purge = %d, want 0", got)
	}
	if err := backend.DeleteTopic(ctx, coord); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}
	if _, err := backend.DescribeTopic(ctx, coord); err == nil {
		t.Fatal("DescribeTopic(deleted) error = nil, want not found")
	}
}

func messageCount(t *testing.T, desc mqgov.TopicDescription) int {
	t.Helper()
	value, err := strconv.Atoi(desc.Config["messages"])
	if err != nil {
		t.Fatalf("invalid messages count in %+v: %v", desc, err)
	}
	return value
}

func getenvDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string) int {
	value := os.Getenv(name)
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}
