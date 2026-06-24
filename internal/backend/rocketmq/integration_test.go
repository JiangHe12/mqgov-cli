//go:build integration

package rocketmq

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestRocketMQIntegration(t *testing.T) {
	namesrv := strings.TrimSpace(os.Getenv("ROCKETMQ_NAMESRV_ADDR"))
	brokerAddr := strings.TrimSpace(os.Getenv("ROCKETMQ_BROKER_ADDR"))
	if namesrv == "" || brokerAddr == "" {
		t.Skip("ROCKETMQ_NAMESRV_ADDR and ROCKETMQ_BROKER_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	backend, err := New(Options{
		NameServers: []string{namesrv},
		BrokerAddr:  brokerAddr,
		Cluster:     "integration",
		Timeout:     15 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	topic := fmt.Sprintf("mqgov-it-%d", time.Now().UnixNano())
	coord := mqgov.TopicCoordinate{Cluster: "integration", Topic: topic}
	defer func() { _ = backend.DeleteTopic(context.Background(), coord) }()

	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: coord, Partitions: 4}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	desc, err := backend.DescribeTopic(ctx, coord)
	if err != nil {
		t.Fatalf("DescribeTopic() error = %v", err)
	}
	if desc.Partitions == 0 {
		t.Fatalf("DescribeTopic().Partitions = 0, want queues")
	}
	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: coord, Partitions: 4}); apperrors.AsAppError(err).Code != apperrors.CodeResourceAlreadyExists {
		t.Fatalf("CreateTopic(existing) error = %v, want ResourceAlreadyExists", err)
	}
	if _, err := backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: coord, Key: []byte("k"), Body: []byte("body")}); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}

	queue := firstQueue(t, ctx, backend, topic)
	if _, err := backend.Peek(ctx, mqgov.MessagePeekRequest{Coordinate: coord, Partition: queue.QueueId, Offset: 0, Count: 1}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("Peek() error = %v, want NotImplemented", err)
	}
	if _, ok := mqgov.SupportsTail(backend); ok {
		t.Fatalf("SupportsTail() = true, want false")
	}
	if _, ok := mqgov.SupportsOffsets(backend); ok {
		t.Fatalf("SupportsOffsets() = true, want false")
	}
	if _, ok := mqgov.SupportsPartitions(backend); ok {
		t.Fatalf("SupportsPartitions() = true, want false")
	}
	if _, ok := mqgov.SupportsACL(backend); ok {
		t.Fatalf("SupportsACL() = true, want false")
	}
	if err := backend.DeleteTopic(ctx, coord); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}
	if _, err := backend.DescribeTopic(ctx, coord); apperrors.AsAppError(err).Code != apperrors.CodeResourceNotFound {
		t.Fatalf("DescribeTopic(deleted) error = %v, want ResourceNotFound", err)
	}
}

func firstQueue(t *testing.T, ctx context.Context, backend *Broker, topic string) *primitive.MessageQueue {
	t.Helper()
	queues, err := backend.topicQueues(ctx, topic)
	if err != nil {
		t.Fatalf("topicQueues() error = %v", err)
	}
	if len(queues) == 0 {
		t.Fatalf("topicQueues() returned no queues")
	}
	return queues[0]
}
