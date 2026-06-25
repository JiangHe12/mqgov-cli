//go:build integration

package pulsar

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestPulsarIntegration(t *testing.T) {
	if strings.TrimSpace(os.Getenv("PULSAR_SERVICE_URL")) == "" || strings.TrimSpace(os.Getenv("PULSAR_ADMIN_URL")) == "" {
		t.Skip("PULSAR_SERVICE_URL and PULSAR_ADMIN_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	backend, err := New(Options{
		ServiceURL:     os.Getenv("PULSAR_SERVICE_URL"),
		AdminURL:       os.Getenv("PULSAR_ADMIN_URL"),
		Tenant:         getenvDefault("PULSAR_TENANT", "public"),
		Namespace:      getenvDefault("PULSAR_NAMESPACE", "default"),
		Cluster:        "integration",
		Token:          os.Getenv("PULSAR_TOKEN"),
		TLS:            os.Getenv("PULSAR_TLS") == "true",
		CACertFile:     os.Getenv("PULSAR_CA_CERT_FILE"),
		ClientCertFile: os.Getenv("PULSAR_CLIENT_CERT_FILE"),
		ClientKeyFile:  os.Getenv("PULSAR_CLIENT_KEY_FILE"),
		Timeout:        15 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	partitionedTopic := fmt.Sprintf("mqgov-it-part-%d", time.Now().UnixNano())
	partitionedCoord := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "public/default", Topic: partitionedTopic}
	defer func() { _ = backend.DeleteTopic(context.Background(), partitionedCoord) }()

	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: partitionedCoord, Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic(partitioned) error = %v", err)
	}
	if _, err := backend.AlterTopic(ctx, mqgov.TopicAlterRequest{Coordinate: partitionedCoord, Partitions: 3}); err != nil {
		t.Fatalf("AlterTopic() error = %v", err)
	}
	if err := backend.DeleteTopic(ctx, partitionedCoord); err != nil {
		t.Fatalf("DeleteTopic(partitioned) error = %v", err)
	}

	topic := fmt.Sprintf("mqgov-it-%d", time.Now().UnixNano())
	sub := topic + "-sub"
	coord := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "public/default", Topic: topic}
	defer func() { _ = backend.DeleteTopic(context.Background(), coord) }()

	if _, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: coord, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	schemaManager, ok := mqgov.SupportsSchema(backend)
	if !ok || !backend.Capabilities().SupportsSchema {
		t.Fatalf("SupportsSchema() = %v caps=%+v, want true", ok, backend.Capabilities())
	}
	baseSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	nextSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null}]}`
	registered, err := schemaManager.RegisterSchema(ctx, mqgov.SchemaRegisterRequest{Subject: topic, Type: "AVRO", Schema: baseSchema})
	if err != nil {
		t.Fatalf("RegisterSchema() error = %v", err)
	}
	if registered.Subject != topic || registered.SchemaHash == "" {
		t.Fatalf("RegisterSchema() = %+v, want schema metadata", registered)
	}
	schemas, err := schemaManager.ListSchemas(ctx, mqgov.SchemaListOptions{Subject: topic})
	if err != nil {
		t.Fatalf("ListSchemas() error = %v", err)
	}
	if len(schemas) != 1 || schemas[0].Subject != topic {
		t.Fatalf("ListSchemas() = %+v, want %q", schemas, topic)
	}
	schemaDesc, err := schemaManager.DescribeSchema(ctx, mqgov.SchemaDescribeRequest{Subject: topic, Version: "latest"})
	if err != nil {
		t.Fatalf("DescribeSchema() error = %v", err)
	}
	if schemaDesc.Subject != topic || schemaDesc.SchemaHash == "" || schemaDesc.Schema == "" {
		t.Fatalf("DescribeSchema() = %+v, want schema metadata", schemaDesc)
	}
	schemaCheck, err := schemaManager.CheckCompatibility(ctx, mqgov.SchemaCheckRequest{Subject: topic, Type: "AVRO", Schema: nextSchema})
	if err != nil {
		t.Fatalf("CheckCompatibility() error = %v", err)
	}
	if !schemaCheck.Compatible || schemaCheck.SchemaHash == "" {
		t.Fatalf("CheckCompatibility() = %+v, want compatible with hash", schemaCheck)
	}
	evolved, err := schemaManager.RegisterSchema(ctx, mqgov.SchemaRegisterRequest{Subject: topic, Type: "AVRO", Schema: nextSchema})
	if err != nil {
		t.Fatalf("RegisterSchema(evolution) error = %v", err)
	}
	if evolved.SchemaHash != mqgov.SHA256Hex([]byte(nextSchema)) {
		t.Fatalf("RegisterSchema(evolution) = %+v, want next schema hash", evolved)
	}
	if _, err := schemaManager.DeleteSchema(ctx, mqgov.SchemaDeleteRequest{Subject: topic}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("DeleteSchema(soft) error = %v, want NotImplemented", err)
	}
	deletedSchema, err := schemaManager.DeleteSchema(ctx, mqgov.SchemaDeleteRequest{Subject: topic, Permanent: true})
	if err != nil {
		t.Fatalf("DeleteSchema(permanent) error = %v", err)
	}
	if deletedSchema.Subject != topic || !deletedSchema.Permanent {
		t.Fatalf("DeleteSchema(permanent) = %+v, want permanent subject delete", deletedSchema)
	}
	if _, err := schemaManager.DescribeSchema(ctx, mqgov.SchemaDescribeRequest{Subject: topic, Version: "latest"}); apperrors.AsAppError(err).Code != apperrors.CodeResourceNotFound {
		t.Fatalf("DescribeSchema(after delete) error = %v, want ResourceNotFound", err)
	}
	if _, err := backend.AlterTopic(ctx, mqgov.TopicAlterRequest{Coordinate: coord, Partitions: 3}); err == nil {
		t.Fatalf("AlterTopic(non-partitioned) error = nil, want clear error")
	} else {
		appErr := apperrors.AsAppError(err)
		if appErr.Code != apperrors.CodeBackendError || appErr.Message != "cannot update partitions on a non-partitioned Pulsar topic" {
			t.Fatalf("AlterTopic(non-partitioned) error = %v", err)
		}
	}
	consumer, err := backend.client.Subscribe(pulsarclient.ConsumerOptions{
		Topic:                       backend.fqn(topic),
		SubscriptionName:            sub,
		SubscriptionInitialPosition: pulsarclient.SubscriptionPositionEarliest,
	})
	if err != nil {
		t.Fatalf("create subscription error = %v", err)
	}
	consumer.Close()
	if _, err := backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: coord, Key: []byte("k"), Body: []byte("body")}); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	before, err := backend.Lag(ctx, mqgov.GroupCoordinate{Cluster: "integration", Namespace: "public/default", Group: sub}, coord)
	if err != nil {
		t.Fatalf("Lag() error = %v", err)
	}
	if before.Total == 0 {
		t.Fatalf("lag before reset = 0, want backlog: %+v", before)
	}
	peeked, err := backend.Peek(ctx, mqgov.MessagePeekRequest{Coordinate: coord, Count: 1})
	if err != nil {
		t.Fatalf("Peek() error = %v", err)
	}
	if peeked.Count != 1 || len(peeked.Messages) != 1 {
		t.Fatalf("Peek() = %+v, want one fingerprint", peeked)
	}
	afterPeek, err := backend.Lag(ctx, mqgov.GroupCoordinate{Cluster: "integration", Namespace: "public/default", Group: sub}, coord)
	if err != nil {
		t.Fatalf("Lag() after peek error = %v", err)
	}
	if afterPeek.Total != before.Total {
		t.Fatalf("peek changed backlog: before=%d after=%d", before.Total, afterPeek.Total)
	}
	tailer, ok := mqgov.SupportsTail(backend)
	if !ok {
		t.Fatalf("SupportsTail() = false, want true")
	}
	aclManager, ok := mqgov.SupportsACL(backend)
	if !ok {
		t.Fatalf("SupportsACL() = false, want true")
	}
	aclRole := fmt.Sprintf("mqgov-acl-it-%d", time.Now().UnixNano())
	aclResource := backend.opts.Tenant + "/" + backend.opts.Namespace
	aclBinding := mqgov.ACLBinding{
		Principal:    aclRole,
		ResourceType: "namespace",
		ResourceName: aclResource,
		PatternType:  "literal",
		Operation:    "produce",
		Permission:   "allow",
	}
	if err := aclManager.GrantACL(ctx, aclBinding); err != nil {
		t.Fatalf("GrantACL() error = %v", err)
	}
	acls, err := aclManager.ListACLs(ctx, mqgov.ACLFilter{Principal: aclRole, ResourceType: "namespace", ResourceName: aclResource, Operation: "produce"})
	if err != nil {
		t.Fatalf("ListACLs(after grant) error = %v", err)
	}
	if len(acls) != 1 {
		t.Fatalf("ListACLs(after grant) = %+v, want one produce binding", acls)
	}
	if err := aclManager.RevokeACL(ctx, aclBinding); err != nil {
		t.Fatalf("RevokeACL() error = %v", err)
	}
	acls, err = aclManager.ListACLs(ctx, mqgov.ACLFilter{Principal: aclRole, ResourceType: "namespace", ResourceName: aclResource, Operation: "produce"})
	if err != nil {
		t.Fatalf("ListACLs(after revoke) error = %v", err)
	}
	if len(acls) != 0 {
		t.Fatalf("ListACLs(after revoke) = %+v, want none", acls)
	}
	tailReq := mqgov.MessageTailRequest{Coordinate: coord, From: "earliest", MaxMessages: 1}
	firstTail := collectTail(t, ctx, tailer, tailReq)
	secondTail := collectTail(t, ctx, tailer, tailReq)
	if len(firstTail) != 1 || len(secondTail) != 1 {
		t.Fatalf("tail counts = %d/%d, want 1/1", len(firstTail), len(secondTail))
	}
	if firstTail[0] != secondTail[0] || firstTail[0].BodySHA256 != peeked.Messages[0].BodySHA256 {
		t.Fatalf("tail fingerprints = %+v / %+v, peek=%+v", firstTail[0], secondTail[0], peeked.Messages[0])
	}
	afterTail, err := backend.Lag(ctx, mqgov.GroupCoordinate{Cluster: "integration", Namespace: "public/default", Group: sub}, coord)
	if err != nil {
		t.Fatalf("Lag() after tail error = %v", err)
	}
	if afterTail.Total != before.Total {
		t.Fatalf("tail changed backlog: before=%d after=%d", before.Total, afterTail.Total)
	}
	plan, err := backend.PlanOffsetReset(ctx, mqgov.OffsetPlanRequest{
		Group:  mqgov.GroupCoordinate{Cluster: "integration", Namespace: "public/default", Group: sub},
		Topic:  coord,
		To:     "latest",
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("PlanOffsetReset() error = %v", err)
	}
	if plan.Total != before.Total {
		t.Fatalf("plan total = %d, want %d", plan.Total, before.Total)
	}
	if _, err := backend.ResetOffset(ctx, mqgov.OffsetPlanRequest{
		Group: mqgov.GroupCoordinate{Cluster: "integration", Namespace: "public/default", Group: sub},
		Topic: coord,
		To:    "latest",
	}); err != nil {
		t.Fatalf("ResetOffset() error = %v", err)
	}
	afterReset, err := backend.Lag(ctx, mqgov.GroupCoordinate{Cluster: "integration", Namespace: "public/default", Group: sub}, coord)
	if err != nil {
		t.Fatalf("Lag() after reset error = %v", err)
	}
	if afterReset.Total != 0 {
		t.Fatalf("lag after reset = %d, want 0", afterReset.Total)
	}
	if _, err := backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: coord, Body: []byte("again")}); err != nil {
		t.Fatalf("Produce(again) error = %v", err)
	}
	purgePlan, err := backend.PurgeTopic(ctx, mqgov.TopicPurgeRequest{Coordinate: coord, DryRun: true})
	if err != nil {
		t.Fatalf("PurgeTopic dry-run error = %v", err)
	}
	if purgePlan.Total == 0 {
		t.Fatalf("purge plan total = 0, want backlog")
	}
	if _, err := backend.PurgeTopic(ctx, mqgov.TopicPurgeRequest{Coordinate: coord}); err != nil {
		t.Fatalf("PurgeTopic() error = %v", err)
	}

	dlqTopic := topic + "-" + sub + "-DLQ"
	dlqCoord := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "public/default", Topic: dlqTopic}
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
	dlqs, err := dlqManager.ListDLQs(ctx, mqgov.DLQListOptions{Topic: topic, Group: sub})
	if err != nil {
		t.Fatalf("ListDLQs() error = %v", err)
	}
	if len(dlqs) != 1 || dlqs[0].Coordinate.Topic != dlqTopic {
		t.Fatalf("ListDLQs() = %+v, want %q", dlqs, dlqTopic)
	}
	dlqPeek, err := dlqManager.PeekDLQ(ctx, mqgov.DLQPeekRequest{DLQ: dlqCoord, Topic: topic, Group: sub, Count: 1})
	if err != nil {
		t.Fatalf("PeekDLQ() error = %v", err)
	}
	if dlqPeek.Count != 1 || dlqPeek.Messages[0].BodySHA256 != dlqProduced.Fingerprint.BodySHA256 {
		t.Fatalf("PeekDLQ() = %+v, want %+v", dlqPeek, dlqProduced.Fingerprint)
	}
	redrivePlan, err := dlqManager.RedriveDLQ(ctx, mqgov.DLQRedriveRequest{DLQ: dlqCoord, Target: coord, Topic: topic, Group: sub, Count: 1, DryRun: true})
	if err != nil {
		t.Fatalf("RedriveDLQ(dry-run) error = %v", err)
	}
	if redrivePlan.Total != 1 {
		t.Fatalf("RedriveDLQ(dry-run).Total = %d, want 1", redrivePlan.Total)
	}
	redriven, err := dlqManager.RedriveDLQ(ctx, mqgov.DLQRedriveRequest{DLQ: dlqCoord, Target: coord, Topic: topic, Group: sub, Count: 1})
	if err != nil {
		t.Fatalf("RedriveDLQ() error = %v", err)
	}
	if redriven.Total != 1 {
		t.Fatalf("RedriveDLQ().Total = %d, want 1", redriven.Total)
	}
	purgeDLQ, err := dlqManager.PurgeDLQ(ctx, mqgov.DLQPurgeRequest{DLQ: dlqCoord, Topic: topic, Group: sub, DryRun: true})
	if err != nil {
		t.Fatalf("PurgeDLQ(dry-run) error = %v", err)
	}
	if purgeDLQ.Total != 1 {
		t.Fatalf("PurgeDLQ(dry-run).Total = %d, want 1", purgeDLQ.Total)
	}
	if err := backend.DeleteTopic(ctx, coord); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
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

func getenvDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
