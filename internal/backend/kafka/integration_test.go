//go:build integration

package kafka

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestKafkaIntegration(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if strings.TrimSpace(brokers) == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	backend, err := New(Options{
		Brokers:           []string{brokers},
		Cluster:           "integration",
		Username:          os.Getenv("KAFKA_USERNAME"),
		Password:          os.Getenv("KAFKA_PASSWORD"),
		SASLMechanism:     os.Getenv("KAFKA_SASL_MECHANISM"),
		TLS:               os.Getenv("KAFKA_TLS") == "true",
		CACertFile:        os.Getenv("KAFKA_CA_CERT_FILE"),
		ClientCertFile:    os.Getenv("KAFKA_CLIENT_CERT_FILE"),
		ClientKeyFile:     os.Getenv("KAFKA_CLIENT_KEY_FILE"),
		SchemaRegistryURL: os.Getenv("KAFKA_SCHEMA_REGISTRY_URL"),
		TLSPinPath:        filepath.Join(t.TempDir(), "tls_known_hosts"),
		Timeout:           10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	topic := fmt.Sprintf("mqgov_it_%d", time.Now().UnixNano())
	group := topic + "_group"
	coord := mqgov.TopicCoordinate{Cluster: "integration", Topic: topic}
	defer func() { _ = backend.DeleteTopic(context.Background(), coord) }()

	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: coord, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: coord, Partitions: 1}); apperrors.AsAppError(err).Code != apperrors.CodeResourceAlreadyExists {
		t.Fatalf("CreateTopic(existing) error = %v, want %s", err, apperrors.CodeResourceAlreadyExists)
	} else if exit := apperrors.ExitCode(err); exit != 5 {
		t.Fatalf("CreateTopic(existing) exit = %d, want 5", exit)
	}
	desc := waitTopic(t, ctx, backend, coord)
	if desc.Partitions != 1 {
		t.Fatalf("partitions = %d, want 1", desc.Partitions)
	}
	if _, err := backend.AlterTopic(ctx, mqgov.TopicAlterRequest{Coordinate: coord, Partitions: 2}); err != nil {
		t.Fatalf("AlterTopic() error = %v", err)
	}
	desc = waitTopic(t, ctx, backend, coord)
	if desc.Partitions < 2 {
		t.Fatalf("partitions = %d, want at least 2", desc.Partitions)
	}

	produced, err := backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: coord, Key: []byte("k"), Body: []byte("body")})
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	peeked, err := backend.Peek(ctx, mqgov.MessagePeekRequest{Coordinate: coord, Partition: produced.Coordinate.Partition, Offset: produced.Coordinate.Offset, Count: 1})
	if err != nil {
		t.Fatalf("Peek() error = %v", err)
	}
	if peeked.Count != 1 || len(peeked.Messages) != 1 {
		t.Fatalf("peeked = %+v, want one fingerprint", peeked)
	}
	if peeked.Messages[0].BodySHA256 != produced.Fingerprint.BodySHA256 || peeked.Messages[0].KeySHA256 != produced.Fingerprint.KeySHA256 {
		t.Fatalf("peek fingerprint = %+v, want %+v", peeked.Messages[0], produced.Fingerprint)
	}
	tailer, ok := mqgov.SupportsTail(backend)
	if !ok {
		t.Fatalf("SupportsTail() = false, want true")
	}
	tailReq := mqgov.MessageTailRequest{Coordinate: coord, Partition: produced.Coordinate.Partition, From: fmt.Sprintf("offset:%d", produced.Coordinate.Offset), MaxMessages: 1}
	firstTail := collectTail(t, ctx, tailer, tailReq)
	secondTail := collectTail(t, ctx, tailer, tailReq)
	if len(firstTail) != 1 || len(secondTail) != 1 {
		t.Fatalf("tail counts = %d/%d, want 1/1", len(firstTail), len(secondTail))
	}
	if firstTail[0] != secondTail[0] || firstTail[0].BodySHA256 != produced.Fingerprint.BodySHA256 {
		t.Fatalf("tail fingerprints = %+v / %+v, produced=%+v", firstTail[0], secondTail[0], produced.Fingerprint)
	}

	plan, err := backend.PlanOffsetReset(ctx, mqgov.OffsetPlanRequest{
		Group:  mqgov.GroupCoordinate{Cluster: "integration", Group: group},
		Topic:  coord,
		To:     "latest",
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("PlanOffsetReset(latest) error = %v", err)
	}
	if plan.Total == 0 {
		t.Fatalf("plan total = 0, want positive impact: %+v", plan)
	}
	applied, err := backend.ResetOffset(ctx, mqgov.OffsetPlanRequest{
		Group: mqgov.GroupCoordinate{Cluster: "integration", Group: group},
		Topic: coord,
		To:    "latest",
	})
	if err != nil {
		t.Fatalf("ResetOffset(latest) error = %v", err)
	}
	if applied.Total != plan.Total {
		t.Fatalf("applied total = %d, want %d", applied.Total, plan.Total)
	}
	zero, err := backend.PlanOffsetReset(ctx, mqgov.OffsetPlanRequest{
		Group:  mqgov.GroupCoordinate{Cluster: "integration", Group: group},
		Topic:  coord,
		To:     "latest",
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("PlanOffsetReset after reset error = %v", err)
	}
	if zero.Total != 0 {
		t.Fatalf("post-reset plan total = %d, want 0: %+v", zero.Total, zero)
	}

	purge, err := backend.PurgeTopic(ctx, mqgov.TopicPurgeRequest{Coordinate: coord, DryRun: true})
	if err != nil {
		t.Fatalf("PurgeTopic dry-run error = %v", err)
	}
	if purge.Total == 0 {
		t.Fatalf("purge total = 0, want positive impact: %+v", purge)
	}

	dlqCoord := mqgov.TopicCoordinate{Cluster: "integration", Topic: topic + ".dlq"}
	defer func() { _ = backend.DeleteTopic(context.Background(), dlqCoord) }()
	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: dlqCoord, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(DLQ) error = %v", err)
	}
	dlqProduced, err := backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: dlqCoord, Key: []byte("dlq-k"), Body: []byte("dlq-body")})
	if err != nil {
		t.Fatalf("Produce(DLQ) error = %v", err)
	}
	dlqManager, ok := mqgov.SupportsDLQ(backend)
	if !ok {
		t.Fatalf("SupportsDLQ() = false, want true")
	}
	if _, err := dlqManager.ListDLQs(ctx, mqgov.DLQListOptions{}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("ListDLQs() error = %v, want NotImplemented", err)
	}
	dlqPeek, err := dlqManager.PeekDLQ(ctx, mqgov.DLQPeekRequest{DLQ: dlqCoord, Partition: dlqProduced.Coordinate.Partition, Offset: dlqProduced.Coordinate.Offset, Count: 1})
	if err != nil {
		t.Fatalf("PeekDLQ() error = %v", err)
	}
	if dlqPeek.Count != 1 || dlqPeek.Messages[0].BodySHA256 != dlqProduced.Fingerprint.BodySHA256 {
		t.Fatalf("PeekDLQ() = %+v, want produced fingerprint %+v", dlqPeek, dlqProduced.Fingerprint)
	}
	if _, err := dlqManager.RedriveDLQ(ctx, mqgov.DLQRedriveRequest{DLQ: dlqCoord, Target: coord, Count: 1, DryRun: true}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("RedriveDLQ(dry-run) error = %v, want NotImplemented", err)
	}
	dlqPurge, err := dlqManager.PurgeDLQ(ctx, mqgov.DLQPurgeRequest{DLQ: dlqCoord, DryRun: true})
	if err != nil {
		t.Fatalf("PurgeDLQ(dry-run) error = %v", err)
	}
	if dlqPurge.Total != 1 {
		t.Fatalf("PurgeDLQ(dry-run).Total = %d, want 1", dlqPurge.Total)
	}
	if err := backend.DeleteTopic(ctx, coord); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}
}

func TestKafkaSchemaRegistryIntegration(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	srURL := os.Getenv("KAFKA_SCHEMA_REGISTRY_URL")
	if strings.TrimSpace(brokers) == "" || strings.TrimSpace(srURL) == "" {
		t.Skip("KAFKA_BROKERS and KAFKA_SCHEMA_REGISTRY_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	backend, err := New(Options{
		Brokers:           []string{brokers},
		Cluster:           "integration",
		SchemaRegistryURL: srURL,
		Timeout:           10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer backend.client.Close()
	manager, ok := mqgov.SupportsSchema(backend)
	if !ok || !backend.Capabilities().SupportsSchema {
		t.Fatalf("SupportsSchema() = %v caps=%+v, want true", ok, backend.Capabilities())
	}
	subject := fmt.Sprintf("mqgov-it-schema-%d-value", time.Now().UnixNano())
	baseSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	nextSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null}]}`
	defer func() {
		_, _ = manager.DeleteSchema(context.Background(), mqgov.SchemaDeleteRequest{Subject: subject})
		_, _ = manager.DeleteSchema(context.Background(), mqgov.SchemaDeleteRequest{Subject: subject, Permanent: true})
	}()
	registered, err := manager.RegisterSchema(ctx, mqgov.SchemaRegisterRequest{Subject: subject, Type: "AVRO", Schema: baseSchema})
	if err != nil {
		t.Fatalf("RegisterSchema() error = %v", err)
	}
	if registered.Subject != subject || registered.Version != "1" || registered.SchemaHash == "" {
		t.Fatalf("RegisterSchema() = %+v, want version 1 metadata", registered)
	}

	subjects, err := manager.ListSchemas(ctx, mqgov.SchemaListOptions{Subject: subject})
	if err != nil {
		t.Fatalf("ListSchemas() error = %v", err)
	}
	if len(subjects) != 1 || subjects[0].Subject != subject {
		t.Fatalf("ListSchemas() = %+v, want %q", subjects, subject)
	}
	desc, err := manager.DescribeSchema(ctx, mqgov.SchemaDescribeRequest{Subject: subject, Version: "latest"})
	if err != nil {
		t.Fatalf("DescribeSchema() error = %v", err)
	}
	if desc.Subject != subject || desc.Version != "1" || desc.SchemaHash == "" || desc.Schema == "" {
		t.Fatalf("DescribeSchema() = %+v, want registered schema metadata", desc)
	}
	check, err := manager.CheckCompatibility(ctx, mqgov.SchemaCheckRequest{Subject: subject, Version: "latest", Type: "AVRO", Schema: nextSchema})
	if err != nil {
		t.Fatalf("CheckCompatibility() error = %v", err)
	}
	if !check.Compatible || check.SchemaHash == "" {
		t.Fatalf("CheckCompatibility() = %+v, want compatible with hash", check)
	}
	evolved, err := manager.RegisterSchema(ctx, mqgov.SchemaRegisterRequest{Subject: subject, Type: "AVRO", Schema: nextSchema})
	if err != nil {
		t.Fatalf("RegisterSchema(evolution) error = %v", err)
	}
	if evolved.Version != "2" || evolved.SchemaHash != mqgov.SHA256Hex([]byte(nextSchema)) {
		t.Fatalf("RegisterSchema(evolution) = %+v, want version 2", evolved)
	}
	deletedVersion, err := manager.DeleteSchema(ctx, mqgov.SchemaDeleteRequest{Subject: subject, Version: "2"})
	if err != nil {
		t.Fatalf("DeleteSchema(version) error = %v", err)
	}
	if deletedVersion.Subject != subject || deletedVersion.Version != "2" {
		t.Fatalf("DeleteSchema(version) = %+v, want version 2", deletedVersion)
	}
	deletedSubject, err := manager.DeleteSchema(ctx, mqgov.SchemaDeleteRequest{Subject: subject})
	if err != nil {
		t.Fatalf("DeleteSchema(subject) error = %v", err)
	}
	if deletedSubject.Subject != subject || len(deletedSubject.Versions) == 0 {
		t.Fatalf("DeleteSchema(subject) = %+v, want deleted versions", deletedSubject)
	}
}

func collectTail(t *testing.T, ctx context.Context, tailer mqgov.Tailer, req mqgov.MessageTailRequest) []mqgov.MessageFingerprint {
	t.Helper()
	var messages []mqgov.MessageFingerprint
	result, err := tailer.Tail(ctx, req, func(fp mqgov.MessageFingerprint) error {
		messages = append(messages, fp)
		return nil
	})
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	if result.Count != int64(len(messages)) {
		t.Fatalf("Tail() result count = %d, collected=%d", result.Count, len(messages))
	}
	return messages
}

func waitTopic(t *testing.T, ctx context.Context, backend *Broker, coord mqgov.TopicCoordinate) mqgov.TopicDescription {
	t.Helper()
	var lastErr error
	for range 20 {
		desc, err := backend.DescribeTopic(ctx, coord)
		if err == nil {
			return desc
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("DescribeTopic() never succeeded: %v", lastErr)
	return mqgov.TopicDescription{}
}
