package cmd

import (
	"context"
	"strings"

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
	coordinate := topicCoord(f, meta, topic)
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
			Topic:          topic,
			ProtectedTopic: isProtectedTopic(meta, topic, description),
			InternalTopic:  description.Internal || isInternalTopicName(topic),
			Plan:           plan,
		},
	}, nil
}

func declaredTopicTarget(meta mqgovctx.Context, topic string, plan bool) mqclass.Target {
	return mqclass.Target{
		Topic:          topic,
		ProtectedTopic: isProtectedTopic(meta, topic, mqgov.TopicDescription{}),
		InternalTopic:  isInternalTopicName(topic),
		Plan:           plan,
	}
}

func sameTopicClassification(left, right mqclass.Target) bool {
	return left.Topic == right.Topic &&
		left.ProtectedTopic == right.ProtectedTopic &&
		left.InternalTopic == right.InternalTopic &&
		left.Plan == right.Plan
}

func isInternalTopicName(topic string) bool {
	name := strings.ToLower(strings.TrimSpace(topic))
	return strings.HasPrefix(name, "__") || strings.HasPrefix(name, "_system") || strings.Contains(name, "consumer_offsets")
}
