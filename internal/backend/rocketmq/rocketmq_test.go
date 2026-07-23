package rocketmq

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	rocketmqclient "github.com/apache/rocketmq-client-go/v2"
	rmqadmin "github.com/apache/rocketmq-client-go/v2/admin"
	rmqerrors "github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestRocketMQClientLoggerIsSilent(t *testing.T) {
	const marker = "rocketmq-governed-read-success-must-not-leak"
	if os.Getenv("MQGOV_TEST_ROCKETMQ_LOG_HELPER") == "1" {
		rlog.Info(marker, map[string]interface{}{"topic": "orders"})
		return
	}

	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRocketMQClientLoggerIsSilent$")
	command.Env = append(os.Environ(), "MQGOV_TEST_ROCKETMQ_LOG_HELPER=1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("RocketMQ logger helper error = %v, output=%s", err, output)
	}
	if strings.Contains(string(output), marker) {
		t.Fatalf("RocketMQ global logger released third-party diagnostics: %s", output)
	}
}

type startFailureProducer struct {
	rocketmqclient.Producer
	shutdownCalls int
}

type topicListResult struct {
	topics         []string
	err            error
	waitForContext bool
}

type topicQueueResult struct {
	queues         []*primitive.MessageQueue
	err            error
	waitForContext bool
}

type scriptedTopicAdmin struct {
	listResults  []topicListResult
	queueResults []topicQueueResult
	listCalls    int
	queueCalls   int
	createCalls  int
	createErr    error
	deleteCalls  int
	closeCalls   int
	afterList    func(int)
	afterQueue   func(int)
}

func (admin *scriptedTopicAdmin) CreateTopic(context.Context, ...rmqadmin.OptionCreate) error {
	admin.createCalls++
	return admin.createErr
}

func (admin *scriptedTopicAdmin) DeleteTopic(context.Context, ...rmqadmin.OptionDelete) error {
	admin.deleteCalls++
	return nil
}

func (admin *scriptedTopicAdmin) FetchAllTopicList(ctx context.Context) (*rmqadmin.TopicList, error) {
	call := admin.listCalls
	admin.listCalls++
	if admin.afterList != nil {
		admin.afterList(admin.listCalls)
	}
	if len(admin.listResults) == 0 {
		return &rmqadmin.TopicList{}, nil
	}
	if call >= len(admin.listResults) {
		call = len(admin.listResults) - 1
	}
	result := admin.listResults[call]
	if result.waitForContext {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &rmqadmin.TopicList{TopicList: append([]string(nil), result.topics...)}, result.err
}

func (admin *scriptedTopicAdmin) FetchPublishMessageQueues(ctx context.Context, _ string) ([]*primitive.MessageQueue, error) {
	call := admin.queueCalls
	admin.queueCalls++
	if admin.afterQueue != nil {
		admin.afterQueue(admin.queueCalls)
	}
	if len(admin.queueResults) == 0 {
		return nil, rmqerrors.ErrTopicNotExist
	}
	if call >= len(admin.queueResults) {
		call = len(admin.queueResults) - 1
	}
	result := admin.queueResults[call]
	if result.waitForContext {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return result.queues, result.err
}

func (admin *scriptedTopicAdmin) Close() error {
	admin.closeCalls++
	return nil
}

func (*startFailureProducer) Start() error { return errors.New("injected start failure") }

func (producer *startFailureProducer) Shutdown() error {
	producer.shutdownCalls++
	return nil
}

func TestProduceShutsDownProducerWhenStartFails(t *testing.T) {
	t.Parallel()
	producer := &startFailureProducer{}
	backend := &Broker{producerFactory: func() (rocketmqclient.Producer, error) {
		return producer, nil
	}}

	_, err := backend.Produce(t.Context(), mqgov.MessageProduceRequest{Coordinate: mqgov.TopicCoordinate{Topic: "orders"}})
	if err == nil {
		t.Fatal("Produce() error = nil, want start failure")
	}
	if producer.shutdownCalls != 1 {
		t.Fatalf("Shutdown() calls = %d, want 1", producer.shutdownCalls)
	}
}

func TestCreateTopicWaitsForConfirmedRouteAndReturnsActualQueues(t *testing.T) {
	t.Parallel()
	admin := &scriptedTopicAdmin{queueResults: []topicQueueResult{
		{err: rmqerrors.ErrTopicNotExist},
		{err: rmqerrors.ErrTopicNotExist},
		{queues: rocketMQQueues(2)},
	}}
	backend := &Broker{
		opts: Options{BrokerAddr: "127.0.0.1:10911", Timeout: time.Second},
		adminFactory: func() (topicAdmin, error) {
			return admin, nil
		},
		topicPropagationInterval: time.Nanosecond,
	}

	description, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{
		Coordinate: mqgov.TopicCoordinate{Topic: "orders"},
		Partitions: 2,
	})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if description.Partitions != 2 {
		t.Fatalf("CreateTopic() partitions = %d, want 2", description.Partitions)
	}
	if admin.createCalls != 1 {
		t.Fatalf("CreateTopic() admin calls = %d, want 1", admin.createCalls)
	}
	if admin.queueCalls != 3 {
		t.Fatalf("CreateTopic() route calls = %d, want 3", admin.queueCalls)
	}
}

func TestCreateTopicStopsOnNonRetryableConfirmationError(t *testing.T) {
	t.Parallel()
	admin := &scriptedTopicAdmin{queueResults: []topicQueueResult{
		{err: rmqerrors.ErrTopicNotExist},
		{err: errors.New("connection refused")},
	}}
	backend := &Broker{
		opts: Options{BrokerAddr: "127.0.0.1:10911", Timeout: time.Second},
		adminFactory: func() (topicAdmin, error) {
			return admin, nil
		},
		topicPropagationInterval: time.Nanosecond,
	}

	_, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{
		Coordinate: mqgov.TopicCoordinate{Topic: "orders"},
		Partitions: 1,
	})
	appErr := apperrors.AsAppError(err)
	if got := appErr.Code; got != apperrors.CodePartialFailure {
		t.Fatalf("CreateTopic() code = %s, want %s", got, apperrors.CodePartialFailure)
	}
	if appErr.Message != "RocketMQ topic create request returned without a client-reported error but confirmation failed" {
		t.Fatalf("CreateTopic() message = %q, want explicit accepted-but-unconfirmed message", appErr.Message)
	}
	if cause := apperrors.AsAppError(errors.Unwrap(err)).Code; cause != apperrors.CodeBackendUnreachable {
		t.Fatalf("CreateTopic() cause code = %s, want %s", cause, apperrors.CodeBackendUnreachable)
	}
	if admin.queueCalls != 2 {
		t.Fatalf("CreateTopic() route calls = %d, want 2", admin.queueCalls)
	}
}

func TestCreateTopicPropagationWaitIsCancelable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	admin := &scriptedTopicAdmin{queueResults: []topicQueueResult{
		{err: rmqerrors.ErrTopicNotExist},
		{waitForContext: true},
	}}
	admin.afterQueue = func(calls int) {
		if calls == 2 {
			cancel()
		}
	}
	backend := &Broker{
		opts: Options{BrokerAddr: "127.0.0.1:10911", Timeout: time.Second},
		adminFactory: func() (topicAdmin, error) {
			return admin, nil
		},
		topicPropagationInterval: time.Hour,
	}

	_, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: mqgov.TopicCoordinate{Topic: "orders"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateTopic() error = %v, want context cancellation", err)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodePartialFailure {
		t.Fatalf("CreateTopic() code = %s, want %s", got, apperrors.CodePartialFailure)
	}
	if admin.queueCalls != 2 {
		t.Fatalf("CreateTopic() route calls = %d, want 2", admin.queueCalls)
	}
}

func TestCreateTopicPropagationTimeoutReportsPartialFailureWithNotFoundCause(t *testing.T) {
	t.Parallel()
	admin := &scriptedTopicAdmin{queueResults: []topicQueueResult{
		{err: rmqerrors.ErrTopicNotExist},
		{waitForContext: true},
	}}
	backend := &Broker{
		opts: Options{BrokerAddr: "127.0.0.1:10911", Timeout: 20 * time.Millisecond},
		adminFactory: func() (topicAdmin, error) {
			return admin, nil
		},
		topicPropagationInterval: time.Hour,
	}

	_, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{
		Coordinate: mqgov.TopicCoordinate{Topic: "orders"},
		Partitions: 1,
	})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodePartialFailure {
		t.Fatalf("CreateTopic() code = %s, want %s", got, apperrors.CodePartialFailure)
	}
	if cause := apperrors.AsAppError(errors.Unwrap(err)).Code; cause != apperrors.CodeResourceNotFound {
		t.Fatalf("CreateTopic() cause code = %s, want %s", cause, apperrors.CodeResourceNotFound)
	}
	if admin.queueCalls != 2 {
		t.Fatalf("CreateTopic() route calls = %d, want 2", admin.queueCalls)
	}
}

func TestCreateTopicRequestErrorIsUncertain(t *testing.T) {
	t.Parallel()
	admin := &scriptedTopicAdmin{
		queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}},
		createErr:    errors.New("connection refused"),
	}
	backend := &Broker{
		opts: Options{BrokerAddr: "127.0.0.1:10911", Timeout: time.Second},
		adminFactory: func() (topicAdmin, error) {
			return admin, nil
		},
	}

	_, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{Coordinate: mqgov.TopicCoordinate{Topic: "orders"}})
	appErr := apperrors.AsAppError(err)
	if got := appErr.Code; got != apperrors.CodePartialFailure {
		t.Fatalf("CreateTopic() code = %s, want %s", got, apperrors.CodePartialFailure)
	}
	if appErr.Message != "RocketMQ topic create request outcome is uncertain" {
		t.Fatalf("CreateTopic() message = %q", appErr.Message)
	}
	if cause := apperrors.AsAppError(errors.Unwrap(err)).Code; cause != apperrors.CodeBackendUnreachable {
		t.Fatalf("CreateTopic() cause code = %s, want %s", cause, apperrors.CodeBackendUnreachable)
	}
}

func TestCreateTopicPreflightFailuresAreKnownNotCommitted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		result   topicQueueResult
		wantCode apperrors.Code
	}{
		{name: "already exists", result: topicQueueResult{queues: []*primitive.MessageQueue{{Topic: "orders"}}}, wantCode: apperrors.CodeResourceAlreadyExists},
		{name: "route lookup failed", result: topicQueueResult{err: errors.New("connection refused")}, wantCode: apperrors.CodeBackendUnreachable},
		{name: "network error containing not-found text", result: topicQueueResult{err: errors.New("connection refused: topic not exist")}, wantCode: apperrors.CodeBackendUnreachable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			admin := &scriptedTopicAdmin{queueResults: []topicQueueResult{test.result}}
			backend := &Broker{
				opts: Options{BrokerAddr: "127.0.0.1:10911", Timeout: time.Second},
				adminFactory: func() (topicAdmin, error) {
					return admin, nil
				},
			}
			_, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{Coordinate: mqgov.TopicCoordinate{Topic: "orders"}})
			if got := apperrors.AsAppError(err).Code; got != test.wantCode {
				t.Fatalf("CreateTopic() code = %s, want %s", got, test.wantCode)
			}
			if admin.createCalls != 0 {
				t.Fatalf("CreateTopic() create calls = %d before successful preflight, want 0", admin.createCalls)
			}
		})
	}
}

func TestCreateTopicRejectsUnconfirmedQueueCount(t *testing.T) {
	t.Parallel()
	admin := &scriptedTopicAdmin{queueResults: []topicQueueResult{
		{err: rmqerrors.ErrTopicNotExist},
		{queues: rocketMQQueues(1)},
	}}
	backend := &Broker{
		opts: Options{BrokerAddr: "127.0.0.1:10911", Timeout: time.Second},
		adminFactory: func() (topicAdmin, error) {
			return admin, nil
		},
	}

	_, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{
		Coordinate: mqgov.TopicCoordinate{Topic: "orders"},
		Partitions: 2,
	})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodePartialFailure {
		t.Fatalf("CreateTopic() code = %s, want %s", got, apperrors.CodePartialFailure)
	}
	if cause := apperrors.AsAppError(errors.Unwrap(err)).Code; cause != apperrors.CodeConflict {
		t.Fatalf("CreateTopic() cause code = %s, want %s", cause, apperrors.CodeConflict)
	}
}

func TestCreateTopicChecksEveryNameServerBeforeAndAfterMutation(t *testing.T) {
	t.Parallel()
	nameServers := []string{"ns1:9876", "ns2:9876"}
	steps := []struct {
		nameServers []string
		admin       *scriptedTopicAdmin
	}{
		{nameServers: nameServers[:1], admin: &scriptedTopicAdmin{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}}},
		{nameServers: nameServers[1:], admin: &scriptedTopicAdmin{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}}},
		{nameServers: nameServers, admin: &scriptedTopicAdmin{}},
		{nameServers: nameServers[:1], admin: &scriptedTopicAdmin{queueResults: []topicQueueResult{{queues: rocketMQQueues(2)}}}},
		{nameServers: nameServers[1:], admin: &scriptedTopicAdmin{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}}},
		{nameServers: nameServers[:1], admin: &scriptedTopicAdmin{queueResults: []topicQueueResult{{queues: rocketMQQueues(2)}}}},
		{nameServers: nameServers[1:], admin: &scriptedTopicAdmin{queueResults: []topicQueueResult{{queues: rocketMQQueues(2)}}}},
	}
	next := 0
	backend := &Broker{
		opts: Options{NameServers: nameServers, BrokerAddr: "127.0.0.1:10911", Timeout: time.Second},
		adminFactoryForNameServers: func(got []string) (topicAdmin, error) {
			if next >= len(steps) {
				t.Fatalf("unexpected admin factory call %d for %v", next+1, got)
			}
			step := steps[next]
			next++
			if !slices.Equal(got, step.nameServers) {
				t.Fatalf("admin factory call %d name servers = %v, want %v", next, got, step.nameServers)
			}
			return step.admin, nil
		},
		topicPropagationInterval: time.Nanosecond,
	}

	description, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{
		Coordinate: mqgov.TopicCoordinate{Topic: "orders"},
		Partitions: 2,
	})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if description.Partitions != 2 || next != len(steps) {
		t.Fatalf("CreateTopic() description = %+v, factory calls = %d, want partitions=2 calls=%d", description, next, len(steps))
	}
	for index, step := range steps {
		if step.admin.closeCalls != 1 {
			t.Fatalf("admin %d Close() calls = %d, want 1", index, step.admin.closeCalls)
		}
	}
}

func TestCreateTopicMultiNameServerPreflightFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		second   topicQueueResult
		wantCode apperrors.Code
	}{
		{name: "topic exists on second name server", second: topicQueueResult{queues: rocketMQQueues(1)}, wantCode: apperrors.CodeResourceAlreadyExists},
		{name: "second name server unavailable", second: topicQueueResult{err: errors.New("connection refused")}, wantCode: apperrors.CodeBackendUnreachable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			admins := []*scriptedTopicAdmin{
				{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}},
				{queueResults: []topicQueueResult{test.second}},
			}
			calls := 0
			backend := &Broker{
				opts: Options{NameServers: []string{"ns1:9876", "ns2:9876"}, BrokerAddr: "127.0.0.1:10911"},
				adminFactoryForNameServers: func([]string) (topicAdmin, error) {
					admin := admins[calls]
					calls++
					return admin, nil
				},
			}
			_, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{Coordinate: mqgov.TopicCoordinate{Topic: "orders"}})
			if got := apperrors.AsAppError(err).Code; got != test.wantCode {
				t.Fatalf("CreateTopic() code = %s, want %s", got, test.wantCode)
			}
			if calls != 2 || admins[0].closeCalls != 1 || admins[1].closeCalls != 1 {
				t.Fatalf("preflight calls = %d, close calls = [%d %d], want 2 and [1 1]", calls, admins[0].closeCalls, admins[1].closeCalls)
			}
		})
	}
}

func TestCreateTopicMultiNameServerPreflightStopsAfterCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	first := &scriptedTopicAdmin{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}}
	first.afterQueue = func(int) { cancel() }
	calls := 0
	backend := &Broker{
		opts: Options{NameServers: []string{"ns1:9876", "ns2:9876"}, BrokerAddr: "127.0.0.1:10911"},
		adminFactoryForNameServers: func([]string) (topicAdmin, error) {
			calls++
			return first, nil
		},
	}

	_, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: mqgov.TopicCoordinate{Topic: "orders"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateTopic() error = %v, want context cancellation", err)
	}
	if calls != 1 {
		t.Fatalf("admin factory calls = %d after cancellation, want 1", calls)
	}
}

func TestCreateTopicDoesNotDispatchWhenCanceledAfterPreflight(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	preflightAdmin := &scriptedTopicAdmin{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}}
	mutationAdmin := &scriptedTopicAdmin{}
	calls := 0
	backend := &Broker{
		opts: Options{BrokerAddr: "127.0.0.1:10911"},
		adminFactory: func() (topicAdmin, error) {
			calls++
			if calls == 1 {
				return preflightAdmin, nil
			}
			cancel()
			return mutationAdmin, nil
		},
	}

	_, err := backend.CreateTopic(ctx, mqgov.TopicCreateRequest{Coordinate: mqgov.TopicCoordinate{Topic: "orders"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateTopic() error = %v, want context cancellation", err)
	}
	if mutationAdmin.createCalls != 0 {
		t.Fatalf("CreateTopic() mutation calls = %d after cancellation, want 0", mutationAdmin.createCalls)
	}
	if mutationAdmin.closeCalls != 1 {
		t.Fatalf("CreateTopic() mutation admin Close() calls = %d, want 1", mutationAdmin.closeCalls)
	}
}

func TestCreateTopicDoesNotSucceedUntilEveryNameServerConfirms(t *testing.T) {
	t.Parallel()
	nameServers := []string{"ns1:9876", "ns2:9876"}
	steps := []*scriptedTopicAdmin{
		{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}},
		{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}},
		{},
		{queueResults: []topicQueueResult{{queues: rocketMQQueues(1)}}},
		{queueResults: []topicQueueResult{{err: rmqerrors.ErrTopicNotExist}}},
	}
	next := 0
	backend := &Broker{
		opts: Options{NameServers: nameServers, BrokerAddr: "127.0.0.1:10911", Timeout: 20 * time.Millisecond},
		adminFactoryForNameServers: func([]string) (topicAdmin, error) {
			admin := steps[next]
			next++
			return admin, nil
		},
		topicPropagationInterval: time.Hour,
	}

	_, err := backend.CreateTopic(t.Context(), mqgov.TopicCreateRequest{
		Coordinate: mqgov.TopicCoordinate{Topic: "orders"},
		Partitions: 1,
	})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodePartialFailure {
		t.Fatalf("CreateTopic() code = %s, want %s", got, apperrors.CodePartialFailure)
	}
	if next != len(steps) {
		t.Fatalf("admin factory calls = %d, want %d", next, len(steps))
	}
}

func TestDeleteTopicFailsClosedWithoutCallingAdmin(t *testing.T) {
	t.Parallel()
	admin := &scriptedTopicAdmin{}
	backend := &Broker{adminFactory: func() (topicAdmin, error) { return admin, nil }}

	err := backend.DeleteTopic(t.Context(), mqgov.TopicCoordinate{Topic: "orders"})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("DeleteTopic() code = %s, want %s", got, apperrors.CodeNotImplemented)
	}
	if admin.deleteCalls != 0 || admin.queueCalls != 0 {
		t.Fatalf("DeleteTopic() admin calls = delete:%d route:%d, want none", admin.deleteCalls, admin.queueCalls)
	}
}

func rocketMQQueues(count int) []*primitive.MessageQueue {
	queues := make([]*primitive.MessageQueue, count)
	for index := range queues {
		queues[index] = &primitive.MessageQueue{Topic: "orders", BrokerName: "broker-a", QueueId: index}
	}
	return queues
}

func TestCapabilitiesAreHonestPartialImplementation(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}

	caps := backend.Capabilities()
	if caps.SupportsOffsets || caps.SupportsPartitions || caps.SupportsACL {
		t.Fatalf("capabilities = %+v, want all optional capabilities false", caps)
	}
	if _, ok := mqgov.SupportsACL(backend); ok {
		t.Fatalf("SupportsACL capability assertion = true, want false")
	}
	if contains(caps.Verbs, "delete") {
		t.Fatalf("Verbs = %v, want unsafe RocketMQ topic delete omitted", caps.Verbs)
	}
}

func TestACLCapabilityFailsClosed(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}

	caps := backend.Capabilities()
	if caps.SupportsACL {
		t.Fatalf("SupportsACL = true, want false")
	}
	if contains(caps.ResourceTypes, "acl") {
		t.Fatalf("ResourceTypes = %v, want no acl resource", caps.ResourceTypes)
	}
	if contains(caps.Verbs, "grant-acl") || contains(caps.Verbs, "revoke-acl") {
		t.Fatalf("Verbs = %v, want no ACL mutation verbs", caps.Verbs)
	}
	if _, ok := mqgov.SupportsACL(backend); ok {
		t.Fatalf("RocketMQ implements ACLManager; want fail-closed SupportsACL gate")
	}
}

func TestSchemaCapabilityFailsClosed(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}

	caps := backend.Capabilities()
	if caps.SupportsSchema {
		t.Fatalf("SupportsSchema = true, want false")
	}
	if contains(caps.ResourceTypes, "schema") {
		t.Fatalf("ResourceTypes = %v, want no schema resource", caps.ResourceTypes)
	}
	if contains(caps.Verbs, "check-schema") {
		t.Fatalf("Verbs = %v, want no schema verbs", caps.Verbs)
	}
	if _, ok := mqgov.SupportsSchema(backend); ok {
		t.Fatalf("RocketMQ implements SchemaManager; want fail-closed SupportsSchema gate")
	}
}

func TestUnsupportedGroupOperationsFailClosed(t *testing.T) {
	backend := &Broker{}

	_, err := backend.ListGroups(context.Background(), mqgov.GroupListOptions{})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("ListGroups() code = %s, want %s", got, apperrors.CodeNotImplemented)
	}
	if _, err := backend.CreateGroup(context.Background(), mqgov.GroupCoordinate{}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("CreateGroup() error = %v, want NotImplemented", err)
	}
	if err := backend.DeleteGroup(context.Background(), mqgov.GroupCoordinate{}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("DeleteGroup() error = %v, want NotImplemented", err)
	}
}

func TestInternalTopicDetection(t *testing.T) {
	for _, topic := range []string{"RMQ_SYS_TRACE_TOPIC", "%RETRY%orders", "%DLQ%orders", "SCHEDULE_TOPIC_XXXX"} {
		if !isInternalTopic(topic) {
			t.Fatalf("isInternalTopic(%q) = false, want true", topic)
		}
	}
	if isInternalTopic("orders") {
		t.Fatalf("isInternalTopic(orders) = true, want false")
	}
}

func TestNewRejectsUnsupportedTLS(t *testing.T) {
	_, err := New(Options{NameServers: []string{"127.0.0.1:9876"}, TLS: true})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("New(TLS) code = %s, want %s", got, apperrors.CodeNotImplemented)
	}
}

func TestNewRejectsNamespaceAndNormalizesWhitespace(t *testing.T) {
	t.Parallel()
	_, err := New(Options{NameServers: []string{"127.0.0.1:9876"}, Namespace: "tenant-a"})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("New(namespace) code = %s, want %s", got, apperrors.CodeNotImplemented)
	}

	backend, err := New(Options{NameServers: []string{"127.0.0.1:9876"}, Namespace: " \t "})
	if err != nil {
		t.Fatalf("New(whitespace namespace) error = %v", err)
	}
	if backend.opts.Namespace != "" {
		t.Fatalf("New(whitespace namespace) stored namespace = %q, want empty", backend.opts.Namespace)
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
