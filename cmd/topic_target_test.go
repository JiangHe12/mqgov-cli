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
