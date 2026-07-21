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

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestRocketMQIntegration(t *testing.T) {
	namesrv := strings.TrimSpace(os.Getenv("ROCKETMQ_NAMESRV_ADDR"))
	brokerAddr := strings.TrimSpace(os.Getenv("ROCKETMQ_BROKER_ADDR"))
	if namesrv == "" || brokerAddr == "" {
		skipOrFailIntegration(t, "ROCKETMQ_NAMESRV_ADDR and ROCKETMQ_BROKER_ADDR not set")
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

	created, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: coord, Partitions: 4})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if created.Partitions != 4 {
		t.Fatalf("CreateTopic().Partitions = %d, want 4 confirmed queues", created.Partitions)
	}
	desc, err := backend.DescribeTopic(ctx, coord)
	if err != nil {
		t.Fatalf("DescribeTopic() error = %v", err)
	}
	if desc.Partitions != 4 || desc.Partitions != created.Partitions {
		t.Fatalf("DescribeTopic().Partitions = %d, want created queue count %d", desc.Partitions, created.Partitions)
	}
	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: coord, Partitions: 4}); apperrors.AsAppError(err).Code != apperrors.CodeResourceAlreadyExists {
		t.Fatalf("CreateTopic(existing) error = %v, want ResourceAlreadyExists", err)
	}
	if _, err := backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: coord, Key: []byte("k"), Body: []byte("body")}); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}

	dlqTopic := "%DLQ%" + topic + "_group"
	dlqCoord := mqgov.TopicCoordinate{Cluster: "integration", Topic: dlqTopic}
	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: dlqCoord, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(DLQ) error = %v", err)
	}
	dlqManager, ok := mqgov.SupportsDLQ(backend)
	if !ok {
		t.Fatalf("SupportsDLQ() = false, want true")
	}
	dlqs, err := dlqManager.ListDLQs(ctx, mqgov.DLQListOptions{Group: topic + "_group"})
	if err != nil {
		t.Fatalf("ListDLQs() error = %v", err)
	}
	if len(dlqs) != 1 || dlqs[0].Coordinate.Topic != dlqTopic {
		t.Fatalf("ListDLQs() = %+v, want %q", dlqs, dlqTopic)
	}
	if _, err := dlqManager.PeekDLQ(ctx, mqgov.DLQPeekRequest{DLQ: dlqCoord}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("PeekDLQ() error = %v, want NotImplemented", err)
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
	if err := backend.DeleteTopic(ctx, coord); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("DeleteTopic() error = %v, want NotImplemented", err)
	}
}

func skipOrFailIntegration(t *testing.T, reason string) {
	t.Helper()
	if strings.EqualFold(strings.TrimSpace(os.Getenv("MQGOV_INTEGRATION_REQUIRED")), "true") {
		t.Fatalf("%s while MQGOV_INTEGRATION_REQUIRED=true", reason)
	}
	t.Skip(reason)
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
