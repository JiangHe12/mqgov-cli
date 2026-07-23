//go:build integration

package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestRabbitMQIntegration(t *testing.T) {
	if strings.TrimSpace(os.Getenv("RABBITMQ_AMQP_URL")) == "" && strings.TrimSpace(os.Getenv("RABBITMQ_HOST")) == "" {
		skipOrFailIntegration(t, "RABBITMQ_AMQP_URL or RABBITMQ_HOST not set")
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
		TLSPinPath:     filepath.Join(t.TempDir(), "tls_known_hosts"),
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

	_, err = backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: coord, Body: []byte("body")})
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
	if _, err := backend.Peek(ctx, mqgov.MessagePeekRequest{Coordinate: coord, Count: 1}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("Peek() error = %v, want %s", err, apperrors.CodeNotImplemented)
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
	aclManager, ok := mqgov.SupportsACL(backend)
	if !ok {
		t.Fatalf("SupportsACL() = false, want true")
	}
	aclPrincipal := fmt.Sprintf("mqgov_acl_it_%d", time.Now().UnixNano())
	aclPassword := fmt.Sprintf("mqgov_acl_pw_%d", time.Now().UnixNano())
	createRabbitMQIntegrationUser(t, ctx, backend, aclPrincipal, aclPassword)
	defer deleteRabbitMQIntegrationUser(t, backend, aclPrincipal)
	aclBinding := mqgov.ACLBinding{
		Principal:    aclPrincipal,
		Vhost:        backend.vhost(),
		ResourceType: "vhost",
		ResourceName: "^" + queue + "$",
		PatternType:  "regex",
		Operation:    "read",
		Permission:   "allow",
	}
	if err := aclManager.GrantACL(ctx, aclBinding); err != nil {
		t.Fatalf("GrantACL() error = %v", err)
	}
	acls, err := aclManager.ListACLs(ctx, mqgov.ACLFilter{Principal: aclPrincipal, Vhost: backend.vhost(), ResourceName: aclBinding.ResourceName, Operation: "read"})
	if err != nil {
		t.Fatalf("ListACLs(after grant) error = %v", err)
	}
	if len(acls) != 1 {
		t.Fatalf("ListACLs(after grant) = %+v, want one read binding", acls)
	}
	if err := aclManager.RevokeACL(ctx, aclBinding); err != nil {
		t.Fatalf("RevokeACL() error = %v", err)
	}
	acls, err = aclManager.ListACLs(ctx, mqgov.ACLFilter{Principal: aclPrincipal, Vhost: backend.vhost(), ResourceName: aclBinding.ResourceName, Operation: "read"})
	if err != nil {
		t.Fatalf("ListACLs(after revoke) error = %v", err)
	}
	if len(acls) != 0 {
		t.Fatalf("ListACLs(after revoke) = %+v, want none", acls)
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

	dlq := queue + ".dlq"
	source := queue + ".source"
	sourceCoord := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "/", Topic: source}
	dlqCoord := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "/", Topic: dlq}
	defer func() { _ = backend.DeleteTopic(context.Background(), sourceCoord) }()
	defer func() { _ = backend.DeleteTopic(context.Background(), dlqCoord) }()
	declareRabbitMQDLXQueues(t, ctx, backend, source, dlq)
	_, err = backend.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: sourceCoord, Body: []byte("dead")})
	if err != nil {
		t.Fatalf("Produce(source for DLQ) error = %v", err)
	}
	deadLetterOne(t, ctx, backend, source)
	waitRabbitMQMessages(t, ctx, backend, dlqCoord, 1)
	dlqManager, ok := mqgov.SupportsDLQ(backend)
	if !ok {
		t.Fatalf("SupportsDLQ() = false, want true")
	}
	dlqs, err := dlqManager.ListDLQs(ctx, mqgov.DLQListOptions{Pattern: dlq})
	if err != nil {
		t.Fatalf("ListDLQs() error = %v", err)
	}
	if len(dlqs) != 1 || dlqs[0].Coordinate.Topic != dlq {
		t.Fatalf("ListDLQs() = %+v, want %q", dlqs, dlq)
	}
	if _, err := dlqManager.PeekDLQ(ctx, mqgov.DLQPeekRequest{DLQ: dlqCoord, Count: 1}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("PeekDLQ() error = %v, want %s", err, apperrors.CodeNotImplemented)
	}
	dlqDescription, err := backend.DescribeTopic(ctx, dlqCoord)
	if err != nil {
		t.Fatalf("DescribeTopic(DLQ after PeekDLQ) error = %v", err)
	}
	if got := messageCount(t, dlqDescription); got != 1 {
		t.Fatalf("DLQ peek changed message count: got=%d want=1", got)
	}
	redrivePlan, err := dlqManager.RedriveDLQ(ctx, mqgov.DLQRedriveRequest{DLQ: dlqCoord, Target: coord, Count: 1, DryRun: true})
	if err != nil {
		t.Fatalf("RedriveDLQ(dry-run) error = %v", err)
	}
	if redrivePlan.Total != 1 {
		t.Fatalf("RedriveDLQ(dry-run).Total = %d, want 1", redrivePlan.Total)
	}
	missingTarget := mqgov.TopicCoordinate{Cluster: "integration", Namespace: "/", Topic: queue + ".missing-target"}
	if _, err := dlqManager.RedriveDLQ(ctx, mqgov.DLQRedriveRequest{DLQ: dlqCoord, Target: missingTarget, Count: 1}); apperrors.AsAppError(err).Code != apperrors.CodeResourceNotFound {
		t.Fatalf("RedriveDLQ(missing target) error = %v, want ResourceNotFound", err)
	}
	waitRabbitMQMessages(t, ctx, backend, dlqCoord, 1)
	redriven, err := dlqManager.RedriveDLQ(ctx, mqgov.DLQRedriveRequest{DLQ: dlqCoord, Target: coord, Count: 1})
	if err != nil {
		t.Fatalf("RedriveDLQ() error = %v", err)
	}
	if redriven.Total != 1 {
		t.Fatalf("RedriveDLQ().Total = %d, want 1", redriven.Total)
	}
	waitRabbitMQMessages(t, ctx, backend, dlqCoord, 0)
	waitRabbitMQMessages(t, ctx, backend, coord, 1)
	if purgeDLQ, err := dlqManager.PurgeDLQ(ctx, mqgov.DLQPurgeRequest{DLQ: dlqCoord, DryRun: true}); err != nil {
		t.Fatalf("PurgeDLQ(dry-run) error = %v", err)
	} else if purgeDLQ.Total != 0 {
		t.Fatalf("PurgeDLQ(dry-run).Total = %d, want 0", purgeDLQ.Total)
	}
	if err := backend.DeleteTopic(ctx, coord); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}
	if _, err := backend.DescribeTopic(ctx, coord); err == nil {
		t.Fatal("DescribeTopic(deleted) error = nil, want not found")
	}
}

func skipOrFailIntegration(t *testing.T, reason string) {
	t.Helper()
	if strings.EqualFold(strings.TrimSpace(os.Getenv("MQGOV_INTEGRATION_REQUIRED")), "true") {
		t.Fatalf("%s while MQGOV_INTEGRATION_REQUIRED=true", reason)
	}
	t.Skip(reason)
}

func declareRabbitMQDLXQueues(t *testing.T, ctx context.Context, backend *Broker, source, dlq string) {
	t.Helper()
	_, err := withChannel(ctx, backend, func(ch *amqp.Channel) (struct{}, error) {
		if err := ch.ExchangeDeclare(source+".dlx", "direct", true, false, false, false, nil); err != nil {
			return struct{}{}, err
		}
		if _, err := ch.QueueDeclare(dlq, true, false, false, false, amqp.Table{"x-dead-letter-exchange": source + ".dlx"}); err != nil {
			return struct{}{}, err
		}
		if err := ch.QueueBind(dlq, dlq, source+".dlx", false, nil); err != nil {
			return struct{}{}, err
		}
		_, err := ch.QueueDeclare(source, true, false, false, false, amqp.Table{"x-dead-letter-exchange": source + ".dlx", "x-dead-letter-routing-key": dlq})
		return struct{}{}, err
	})
	if err != nil {
		t.Fatalf("declare DLX queues error = %v", err)
	}
}

func deadLetterOne(t *testing.T, ctx context.Context, backend *Broker, queue string) {
	t.Helper()
	_, err := withChannel(ctx, backend, func(ch *amqp.Channel) (struct{}, error) {
		msg, ok, err := ch.Get(queue, false)
		if err != nil {
			return struct{}{}, err
		}
		if !ok {
			return struct{}{}, fmt.Errorf("source queue %s empty", queue)
		}
		return struct{}{}, msg.Nack(false, false)
	})
	if err != nil {
		t.Fatalf("deadLetterOne() error = %v", err)
	}
}

func waitRabbitMQMessages(t *testing.T, ctx context.Context, backend *Broker, coord mqgov.TopicCoordinate, want int) {
	t.Helper()
	var last int
	for range 20 {
		desc, err := backend.DescribeTopic(ctx, coord)
		if err == nil {
			last = messageCount(t, desc)
			if last == want {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("queue %s messages = %d, want %d", coord.Topic, last, want)
}

func messageCount(t *testing.T, desc mqgov.TopicDescription) int {
	t.Helper()
	value, err := strconv.Atoi(desc.Config["messages"])
	if err != nil {
		t.Fatalf("invalid messages count in %+v: %v", desc, err)
	}
	return value
}

func createRabbitMQIntegrationUser(t *testing.T, ctx context.Context, backend *Broker, username, password string) {
	t.Helper()
	body, err := json.Marshal(struct {
		Password string `json:"password"`
		Tags     string `json:"tags"`
	}{Password: password, Tags: ""})
	if err != nil {
		t.Fatalf("Marshal(user) error = %v", err)
	}
	endpoint := strings.TrimRight(backend.manageURL, "/") + "/api/users/" + url.PathEscape(username)
	resp, err := backend.managementRequest(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create RabbitMQ ACL user request error = %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("create RabbitMQ ACL user status = %d, want 201 or 204", resp.StatusCode)
	}
}

func deleteRabbitMQIntegrationUser(t *testing.T, backend *Broker, username string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	endpoint := strings.TrimRight(backend.manageURL, "/") + "/api/users/" + url.PathEscape(username)
	resp, err := backend.managementRequest(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		t.Logf("delete RabbitMQ ACL user request error: %v", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		t.Logf("delete RabbitMQ ACL user status = %d, want 204 or 404", resp.StatusCode)
	}
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
