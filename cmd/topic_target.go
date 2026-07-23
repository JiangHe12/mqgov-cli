package cmd

import (
	"context"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

type resolvedTopicTarget struct {
	Coordinate     mqgov.TopicCoordinate
	Description    mqgov.TopicDescription
	Classification mqclass.Target
}

func resolveTopicTarget(
	ctx context.Context,
	backend mqgov.Broker,
	f *cliFlags,
	meta mqgovctx.Context,
	topic string,
	plan bool,
) (resolvedTopicTarget, error) {
	if err := validateTopicName(topic); err != nil {
		return resolvedTopicTarget{}, err
	}
	coordinate, err := topicCoord(f, meta, backend, topic)
	if err != nil {
		return resolvedTopicTarget{}, err
	}
	description, err := backend.DescribeTopic(ctx, coordinate)
	if err != nil {
		appErr := apperrors.AsAppError(err)
		return resolvedTopicTarget{}, apperrors.New(appErr.Code, "cannot resolve topic metadata before authorization", err)
	}
	if description.Coordinate.Topic != coordinate.Topic ||
		(coordinate.Cluster != "" && description.Coordinate.Cluster != "" && description.Coordinate.Cluster != coordinate.Cluster) ||
		(coordinate.Namespace != "" && description.Coordinate.Namespace != "" && description.Coordinate.Namespace != coordinate.Namespace) {
		return resolvedTopicTarget{}, apperrors.New(apperrors.CodeValidationFailed, "broker returned metadata for a different topic target", nil)
	}
	return resolvedTopicTarget{
		Coordinate:  coordinate,
		Description: description,
		Classification: mqclass.Target{
			Backend:        backend.Describe().Backend,
			Topic:          topic,
			ProtectedTopic: isProtectedTopic(meta, topic, description),
			InternalTopic:  description.Internal || mqclass.IsInternalTopic(backend.Describe().Backend, topic),
			Plan:           plan,
		},
	}, nil
}

func declaredTopicTarget(meta mqgovctx.Context, backend, topic string, plan bool) mqclass.Target {
	return mqclass.Target{
		Backend:        backend,
		Topic:          topic,
		ProtectedTopic: isProtectedTopic(meta, topic, mqgov.TopicDescription{}),
		InternalTopic:  mqclass.IsInternalTopic(backend, topic),
		Plan:           plan,
	}
}

func sameTopicClassification(left, right mqclass.Target) bool {
	return left.Backend == right.Backend &&
		left.Topic == right.Topic &&
		left.ProtectedTopic == right.ProtectedTopic &&
		left.InternalTopic == right.InternalTopic &&
		left.CreateMayAlter == right.CreateMayAlter &&
		left.Plan == right.Plan
}

func canonicalBrokerScope(f *cliFlags, meta mqgovctx.Context, backend mqgov.Broker) (mqgov.Description, error) {
	description := backend.Describe()
	capabilities := backend.Capabilities()
	if description.Backend == "" || capabilities.Backend == "" || description.Backend != capabilities.Backend {
		return mqgov.Description{}, apperrors.New(apperrors.CodeValidationFailed, "backend description and capabilities identify different backends", nil)
	}
	requestedCluster := firstNonEmpty(f.Cluster, meta.Cluster)
	if requestedCluster != "" && description.Cluster != "" && requestedCluster != description.Cluster {
		return mqgov.Description{}, apperrors.New(apperrors.CodeValidationFailed, "requested cluster does not match the backend cluster", nil)
	}
	requestedNamespace := firstNonEmpty(f.Namespace, meta.Namespace)
	if requestedNamespace != "" && description.Namespace != "" && requestedNamespace != description.Namespace {
		return mqgov.Description{}, apperrors.New(apperrors.CodeValidationFailed, "requested namespace does not match the backend namespace", nil)
	}
	description.Cluster = firstNonEmpty(description.Cluster, requestedCluster)
	description.Namespace = firstNonEmpty(description.Namespace, requestedNamespace)
	return description, nil
}
