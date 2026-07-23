package cmd

import (
	"slices"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func newTopicCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "topic", Short: "Manage topics"}
	cmd.AddCommand(newTopicListCmd(f), newTopicDescribeCmd(f), newTopicCreateCmd(f), newTopicAlterCmd(f), newTopicDeleteCmd(f), newTopicPurgeCmd(f))
	return cmd
}

func newTopicListCmd(f *cliFlags) *cobra.Command {
	var pattern string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List topics",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateExactPattern(pattern); err != nil {
				return err
			}
			options := mqgov.TopicListOptions{Namespace: f.Namespace, Pattern: pattern}
			items, opTarget, err := runMandatoryBrokerList(f, readAuditSpec{
				Action:   "mq.topic.list",
				Target:   audit.EventTarget{ResourceType: "topic"},
				Metadata: mutationValueMetadata("mq.topic.list", options),
			}, func(meta mqgovctx.Context) error {
				return classifyAndAuthorize(f, meta, mqclass.OperationList, mqclass.Target{Topic: pattern}, "")
			}, func(backend mqgov.Broker, _ mqgovctx.Context) ([]mqgov.TopicDescription, error) {
				return backend.ListTopics(cmd.Context(), options)
			})
			if err != nil {
				return err
			}
			if f.Output == "json" {
				return targetJSONList(f, "TopicList", items, len(items), len(items), opTarget)
			}
			rows := make([][]string, 0, len(items))
			for _, item := range items {
				rows = append(rows, []string{item.Coordinate.Topic, strconv.Itoa(item.Partitions), strconv.FormatBool(item.Protected), strconv.FormatBool(item.Internal)})
			}
			return targetTable(f, []string{"TOPIC", "PARTITIONS", "PROTECTED", "INTERNAL"}, rows, opTarget)
		},
	}
	cmd.Flags().StringVar(&pattern, "pattern", "", "Exact topic name")
	return cmd
}

func newTopicDescribeCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "describe TOPIC",
		Short: "Describe a topic",
		Args:  exactArgsWithTopic(1, 0),
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]
			resolved, opTarget, err := runMandatoryBrokerRead(f, readAuditSpec{
				Action:   "mq.topic.describe",
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.describe", map[string]string{"topic": topic}),
			}, func(meta mqgovctx.Context) error {
				target := declaredTopicTarget(meta, firstNonEmpty(f.Backend, meta.Backend, defaultFakeBackend), topic, false)
				return classifyAndAuthorize(f, meta, mqclass.OperationDescribe, target, "")
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (resolvedTopicTarget, error) {
				resolved, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
				if resolveErr != nil {
					return resolvedTopicTarget{}, resolveErr
				}
				if authorizeErr := classifyAndAuthorize(f, meta, mqclass.OperationDescribe, resolved.Classification, ""); authorizeErr != nil {
					return resolvedTopicTarget{}, authorizeErr
				}
				return resolved, nil
			}, func(resolvedTopicTarget) int {
				return 1
			})
			if err != nil {
				return err
			}
			return targetJSONData(f, "TopicDescription", resolved.Description, opTarget, operationTargetRead)
		},
	}
}

func newTopicCreateCmd(f *cliFlags) *cobra.Command {
	var partitions int
	cmd := &cobra.Command{
		Use:   "create TOPIC",
		Short: "Create a topic",
		Args:  exactArgsWithTopic(1, 0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "create", "topic", args[0], map[string]any{"partitions": partitions})
			}
			topic := args[0]
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.topic.create.preflight",
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.create.preflight", map[string]any{"topic": topic, "partitions": partitions}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (topicCreatePreflight, error) {
				coordinate, coordinateErr := topicCoord(f, meta, backend, topic)
				return topicCreatePreflight{Coordinate: coordinate, Backend: backend.Describe().Backend}, coordinateErr
			}, func(topicCreatePreflight) int { return 1 })
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			target, allow := topicCreateAuthorizationTarget(preflight.Context, preflight.Value.Backend, preflight.Value.Coordinate)
			if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationCreateTopic, target, allow); err != nil {
				return err
			}
			request := mqgov.TopicCreateRequest{Coordinate: preflight.Value.Coordinate, Partitions: partitions, Protected: target.ProtectedTopic}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.topic.create",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.create", request),
			})
			if err != nil {
				return err
			}
			desc, operationErr := preflight.Backend.CreateTopic(cmd.Context(), request)
			if err := finishMutationAudit(handle, topicCreateAuditOutcome(operationErr), operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "TopicDescription", desc, preflight.Target, operationTargetWrite)
		},
	}
	cmd.Flags().IntVar(&partitions, "partitions", 1, "Partition count")
	return cmd
}

type topicCreatePreflight struct {
	Coordinate mqgov.TopicCoordinate
	Backend    string
}

type topicPartitionPreflight struct {
	Manager  mqgov.PartitionManager
	Resolved resolvedTopicTarget
}

func topicCreateAuthorizationTarget(meta mqgovctx.Context, backend string, coordinate mqgov.TopicCoordinate) (mqclass.Target, safety.AllowFlag) {
	target := declaredTopicTarget(meta, backend, coordinate.Topic, false)
	target.InternalTopic = target.InternalTopic || mqclass.IsInternalTopicScope(backend, coordinate.Namespace, coordinate.Topic)
	if backend != "rocketmq" {
		return target, ""
	}
	target.CreateMayAlter = true
	return target, allowTopicUpsert
}

func topicCreateAuditOutcome(operationErr error) mutationAuditOutcome {
	if operationErr != nil && apperrors.AsAppError(operationErr).Code == apperrors.CodePartialFailure {
		return mutationAuditOutcome{Uncertain: 1, counted: true}
	}
	return mutationAuditOutcome{}
}

func newTopicAlterCmd(f *cliFlags) *cobra.Command {
	var partitions int
	cmd := &cobra.Command{
		Use:   "alter TOPIC",
		Short: "Alter topic partitions/config",
		Args:  exactArgsWithTopic(1, 0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "alter", "topic", args[0], map[string]any{"partitions": partitions})
			}
			topic := args[0]
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.topic.alter.preflight",
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.alter.preflight", map[string]any{"topic": topic, "partitions": partitions}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (topicPartitionPreflight, error) {
				manager, ok := mqgov.SupportsPartitions(backend)
				if !ok {
					return topicPartitionPreflight{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support partition management", nil)
				}
				resolved, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
				return topicPartitionPreflight{Manager: manager, Resolved: resolved}, resolveErr
			}, func(topicPartitionPreflight) int { return 1 })
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationAlterTopic, preflight.Value.Resolved.Classification, ""); err != nil {
				return err
			}
			request := mqgov.TopicAlterRequest{Coordinate: preflight.Value.Resolved.Coordinate, Partitions: partitions}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.topic.alter",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.alter", request),
			})
			if err != nil {
				return err
			}
			changed, operationErr := preflight.Value.Manager.AlterTopic(cmd.Context(), request)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "TopicDescription", changed, preflight.Target, operationTargetWrite)
		},
	}
	cmd.Flags().IntVar(&partitions, "partitions", 0, "New partition count")
	return cmd
}

func newTopicDeleteCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete TOPIC",
		Short: "Delete a topic",
		Args:  exactArgsWithTopic(1, 0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				if topicDeletePlanUsesRocketMQ(f) {
					return apperrors.New(apperrors.CodeNotImplemented, "RocketMQ does not support topic delete", nil)
				}
				return printBrokerChangePlan(f, "delete", "topic", args[0], nil)
			}
			topic := args[0]
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.topic.delete.preflight",
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.delete.preflight", map[string]string{"topic": topic}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (resolvedTopicTarget, error) {
				if !slices.Contains(backend.Capabilities().Verbs, "delete") {
					return resolvedTopicTarget{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support topic delete", nil)
				}
				return resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			}, func(resolvedTopicTarget) int { return 1 })
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationDeleteTopic, preflight.Value.Classification, allowTopicDelete); err != nil {
				return err
			}
			coordinate := preflight.Value.Coordinate
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.topic.delete",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.delete", coordinate),
			})
			if err != nil {
				return err
			}
			operationErr := preflight.Backend.DeleteTopic(cmd.Context(), coordinate)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "DeleteResult", map[string]string{"topic": topic, "status": audit.StatusSuccess}, preflight.Target, operationTargetWrite)
		},
	}
}

func topicDeletePlanUsesRocketMQ(f *cliFlags) bool {
	if f.Backend != "" {
		return f.Backend == "rocketmq"
	}
	item, _, err := resolvedContext(f)
	return err == nil && item.Backend == "rocketmq"
}

func newTopicPurgeCmd(f *cliFlags) *cobra.Command {
	var dlq bool
	cmd := &cobra.Command{
		Use:   "purge TOPIC",
		Short: "Purge topic or DLQ messages",
		Args:  exactArgsWithTopic(1, 0),
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]
			op := mqclass.OperationPurgeTopic
			if dlq {
				op = mqclass.OperationPurgeDLQ
			}
			dryRun := f.DryRun || f.Plan
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.topic.purge.preflight",
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.purge.preflight", map[string]any{"topic": topic, "dlq": dlq, "dryRun": dryRun}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (topicPurgePreflight, error) {
				manager, ok := mqgov.SupportsPartitions(backend)
				if !ok {
					return topicPurgePreflight{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support topic purge", nil)
				}
				resolved, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, dryRun)
				if resolveErr != nil {
					return topicPurgePreflight{}, resolveErr
				}
				value := topicPurgePreflight{Manager: manager, Resolved: resolved}
				if !dryRun {
					return value, nil
				}
				if authorizeErr := classifyAndAuthorize(f, meta, op, resolved.Classification, ""); authorizeErr != nil {
					return topicPurgePreflight{}, authorizeErr
				}
				request := mqgov.TopicPurgeRequest{Coordinate: resolved.Coordinate, DLQ: dlq, DryRun: true}
				value.Preview, resolveErr = manager.PurgeTopic(cmd.Context(), request)
				value.HasPreview = resolveErr == nil
				return value, resolveErr
			}, func(value topicPurgePreflight) int {
				if value.HasPreview {
					return int(value.Preview.Fingerprint.Count)
				}
				return 1
			})
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			resolved := preflight.Value.Resolved
			allow := allowTopicPurge
			if dryRun {
				allow = ""
			}
			if !dryRun {
				if err := classifyAndAuthorize(f, preflight.Context, op, resolved.Classification, allow); err != nil {
					return err
				}
			}
			request := mqgov.TopicPurgeRequest{Coordinate: resolved.Coordinate, DLQ: dlq, DryRun: dryRun}
			if dryRun {
				return targetJSONData(f, "TopicPurgeResult", preflight.Value.Preview, preflight.Target, operationTargetWrite)
			}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.topic.purge",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.purge", request),
			})
			if err != nil {
				return err
			}
			result, operationErr := preflight.Value.Manager.PurgeTopic(cmd.Context(), request)
			outcome, total := purgeMutationOutcome(result.BatchOutcome, len(result.Impact), operationErr)
			if err := finishBatchMutationAuditWithOutcome(handle, total, outcome, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "TopicPurgeResult", result, preflight.Target, operationTargetWrite)
		},
	}
	cmd.Flags().BoolVar(&dlq, "dlq", false, "Purge DLQ")
	return cmd
}

type topicPurgePreflight struct {
	Manager    mqgov.PartitionManager
	Resolved   resolvedTopicTarget
	Preview    mqgov.TopicPurgeResult
	HasPreview bool
}

func purgeMutationOutcome(reported mqgov.BatchOutcome, completedUnits int, operationErr error) (mqgov.BatchOutcome, int) {
	if !reported.Empty() {
		return reported, reported.Count()
	}
	outcome := mqgov.BatchOutcome{Succeeded: completedUnits}
	if operationErr != nil {
		outcome.Uncertain = 1
	}
	return outcome, outcome.Count()
}

func topicCoord(f *cliFlags, meta mqgovctx.Context, backend mqgov.Broker, topic string) (mqgov.TopicCoordinate, error) {
	if err := validateTopicName(topic); err != nil {
		return mqgov.TopicCoordinate{}, err
	}
	scope, err := canonicalBrokerScope(f, meta, backend)
	if err != nil {
		return mqgov.TopicCoordinate{}, err
	}
	return mqgov.TopicCoordinate{Cluster: scope.Cluster, Namespace: scope.Namespace, Topic: topic}, nil
}
