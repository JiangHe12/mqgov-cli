package cmd

import (
	"context"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/backend/fake"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

type changingTopicMetadataBroker struct {
	mqgov.Broker
	descriptions []mqgov.TopicDescription
	err          error
	calls        int
}

type backendIdentityBroker struct {
	mqgov.Broker
	backend string
}

func (b *backendIdentityBroker) Describe() mqgov.Description {
	description := b.Broker.Describe()
	description.Backend = b.backend
	return description
}

func (b *backendIdentityBroker) Capabilities() mqgov.Capabilities {
	capabilities := b.Broker.Capabilities()
	capabilities.Backend = b.backend
	return capabilities
}

func (b *changingTopicMetadataBroker) DescribeTopic(context.Context, mqgov.TopicCoordinate) (mqgov.TopicDescription, error) {
	b.calls++
	if b.err != nil {
		return mqgov.TopicDescription{}, b.err
	}
	index := b.calls - 1
	if index >= len(b.descriptions) {
		index = len(b.descriptions) - 1
	}
	return b.descriptions[index], nil
}

func TestResolveTopicTargetBindsSingleMetadataSnapshot(t *testing.T) {
	t.Parallel()
	meta := mqgovctx.Context{Backend: "fake", Cluster: "cluster-a", Namespace: "ns-a"}
	broker := &changingTopicMetadataBroker{
		Broker: fake.New(meta.Cluster, meta.Namespace),
		descriptions: []mqgov.TopicDescription{
			{
				Coordinate: mqgov.TopicCoordinate{Cluster: meta.Cluster, Namespace: meta.Namespace, Topic: "orders"},
				Partitions: 3,
			},
			{
				Coordinate: mqgov.TopicCoordinate{Cluster: meta.Cluster, Namespace: meta.Namespace, Topic: "orders"},
				Partitions: 3,
				Protected:  true,
				Internal:   true,
			},
		},
	}

	resolved, err := resolveTopicTarget(context.Background(), broker, newDefaultFlags(), meta, "orders", false)
	if err != nil {
		t.Fatalf("resolveTopicTarget() error = %v", err)
	}
	if broker.calls != 1 {
		t.Fatalf("DescribeTopic() calls = %d, want exactly 1", broker.calls)
	}
	if resolved.Classification.ProtectedTopic || resolved.Classification.InternalTopic {
		t.Fatalf("classification used metadata outside the bound snapshot: %+v", resolved.Classification)
	}
	if resolved.Coordinate != (mqgov.TopicCoordinate{Cluster: meta.Cluster, Namespace: meta.Namespace, Topic: "orders"}) {
		t.Fatalf("resolved coordinate = %+v", resolved.Coordinate)
	}
}

func TestResolveTopicTargetFailsClosedWhenMetadataUnknown(t *testing.T) {
	t.Parallel()
	broker := &changingTopicMetadataBroker{
		Broker: fake.New("cluster-a", ""),
		err:    apperrors.New(apperrors.CodeBackendUnreachable, "metadata unavailable", nil),
	}

	_, err := resolveTopicTarget(context.Background(), broker, newDefaultFlags(), mqgovctx.Context{Backend: "fake", Cluster: "cluster-a"}, "orders", false)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeBackendUnreachable {
		t.Fatalf("resolveTopicTarget() code = %s, want %s; err=%v", got, apperrors.CodeBackendUnreachable, err)
	}
	if broker.calls != 1 {
		t.Fatalf("DescribeTopic() calls = %d, want exactly 1", broker.calls)
	}
}

func TestResolveTopicTargetUsesBackendScopeWhenContextIsEmpty(t *testing.T) {
	t.Parallel()
	broker := fake.New("cluster-a", "ns-a")

	resolved, err := resolveTopicTarget(context.Background(), broker, newDefaultFlags(), mqgovctx.Context{Backend: "fake"}, "orders", false)
	if err != nil {
		t.Fatalf("resolveTopicTarget() error = %v", err)
	}
	if resolved.Coordinate != (mqgov.TopicCoordinate{Cluster: "cluster-a", Namespace: "ns-a", Topic: "orders"}) {
		t.Fatalf("resolved coordinate = %+v", resolved.Coordinate)
	}
}

func TestResolveTopicTargetRejectsExplicitScopeMismatch(t *testing.T) {
	t.Parallel()
	broker := fake.New("cluster-a", "ns-a")

	_, err := resolveTopicTarget(context.Background(), broker, newDefaultFlags(), mqgovctx.Context{Backend: "fake", Cluster: "cluster-b"}, "orders", false)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
		t.Fatalf("resolveTopicTarget() code = %s, want %s; err=%v", got, apperrors.CodeValidationFailed, err)
	}
}

func TestGroupCoordUsesBackendScopeAndRejectsExplicitMismatch(t *testing.T) {
	t.Parallel()
	broker := fake.New("cluster-a", "ns-a")

	coordinate, err := groupCoord(newDefaultFlags(), mqgovctx.Context{Backend: "fake"}, broker, "billing")
	if err != nil {
		t.Fatalf("groupCoord() error = %v", err)
	}
	if coordinate != (mqgov.GroupCoordinate{Cluster: "cluster-a", Namespace: "ns-a", Group: "billing"}) {
		t.Fatalf("group coordinate = %+v", coordinate)
	}
	if _, err := groupCoord(newDefaultFlags(), mqgovctx.Context{Backend: "fake", Namespace: "ns-b"}, broker, "billing"); apperrors.AsAppError(err).Code != apperrors.CodeValidationFailed {
		t.Fatalf("groupCoord(mismatch) error = %v, want %s", err, apperrors.CodeValidationFailed)
	}
}

func TestRocketMQDeclaredAndResolvedInternalTopicClassificationMatch(t *testing.T) {
	t.Parallel()
	const topic = "%RETRY%billing"
	identity := &backendIdentityBroker{Broker: fake.New("rocketmq", ""), backend: "rocketmq"}
	broker := &changingTopicMetadataBroker{
		Broker: identity,
		descriptions: []mqgov.TopicDescription{{
			Coordinate: mqgov.TopicCoordinate{Cluster: "rocketmq", Topic: topic},
		}},
	}

	declared := declaredTopicTarget(mqgovctx.Context{}, "rocketmq", topic, false)
	resolved, err := resolveTopicTarget(context.Background(), broker, newDefaultFlags(), mqgovctx.Context{}, topic, false)
	if err != nil {
		t.Fatalf("resolveTopicTarget() error = %v", err)
	}
	if !declared.InternalTopic || !resolved.Classification.InternalTopic {
		t.Fatalf("internal classification = declared:%+v resolved:%+v", declared, resolved.Classification)
	}
	if !sameTopicClassification(declared, resolved.Classification) {
		t.Fatalf("declared/resolved classification mismatch: declared=%+v resolved=%+v", declared, resolved.Classification)
	}
}

func TestResolveTopicTargetRejectsMismatchedMetadata(t *testing.T) {
	t.Parallel()
	broker := &changingTopicMetadataBroker{
		Broker: fake.New("cluster-a", ""),
		descriptions: []mqgov.TopicDescription{{
			Coordinate: mqgov.TopicCoordinate{Cluster: "cluster-a", Topic: "payments"},
		}},
	}

	_, err := resolveTopicTarget(context.Background(), broker, newDefaultFlags(), mqgovctx.Context{Backend: "fake", Cluster: "cluster-a"}, "orders", false)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
		t.Fatalf("resolveTopicTarget() code = %s, want %s; err=%v", got, apperrors.CodeValidationFailed, err)
	}
}
