package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"

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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			if err := classifyAndAuthorize(f, meta, mqclass.OperationList, mqclass.Target{Topic: pattern}, ""); err != nil {
				return err
			}
			items, err := backend.ListTopics(cmd.Context(), mqgov.TopicListOptions{Namespace: f.Namespace, Pattern: pattern})
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic"}, audit.StatusSuccess, fmt.Sprintf("list count=%d", len(items)), nil)
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
	cmd.Flags().StringVar(&pattern, "pattern", "", "Exact topic name or wildcard pattern")
	return cmd
}

func newTopicDescribeCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "describe TOPIC",
		Short: "Describe a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			topic := args[0]
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			if err != nil {
				return err
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationDescribe, resolved.Classification, ""); err != nil {
				return err
			}
			appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusSuccess, "describe", nil)
			return targetJSONData(f, "TopicDescription", resolved.Description, opTarget, operationTargetRead)
		},
	}
}

func newTopicCreateCmd(f *cliFlags) *cobra.Command {
	var partitions int
	cmd := &cobra.Command{
		Use:   "create TOPIC",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "create", "topic", args[0], map[string]any{"partitions": partitions})
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			topic := args[0]
			target := declaredTopicTarget(meta, topic, false)
			if err := classifyAndAuthorize(f, meta, mqclass.OperationCreateTopic, target, ""); err != nil {
				return err
			}
			request := mqgov.TopicCreateRequest{Coordinate: topicCoord(f, meta, topic), Partitions: partitions, Protected: target.ProtectedTopic}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.topic.create",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.create", request),
			})
			if err != nil {
				return err
			}
			desc, operationErr := backend.CreateTopic(cmd.Context(), request)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "TopicDescription", desc, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().IntVar(&partitions, "partitions", 1, "Partition count")
	return cmd
}

func newTopicAlterCmd(f *cliFlags) *cobra.Command {
	var partitions int
	cmd := &cobra.Command{
		Use:   "alter TOPIC",
		Short: "Alter topic partitions/config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "alter", "topic", args[0], map[string]any{"partitions": partitions})
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsPartitions(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support partition management", nil)
			}
			topic := args[0]
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			if err != nil {
				return err
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationAlterTopic, resolved.Classification, ""); err != nil {
				return err
			}
			request := mqgov.TopicAlterRequest{Coordinate: resolved.Coordinate, Partitions: partitions}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.topic.alter",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.alter", request),
			})
			if err != nil {
				return err
			}
			changed, operationErr := manager.AlterTopic(cmd.Context(), request)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "TopicDescription", changed, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().IntVar(&partitions, "partitions", 0, "New partition count")
	return cmd
}

func newTopicDeleteCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete TOPIC",
		Short: "Delete a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "delete", "topic", args[0], nil)
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			topic := args[0]
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			if err != nil {
				return err
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationDeleteTopic, resolved.Classification, allowTopicDelete); err != nil {
				return err
			}
			coordinate := resolved.Coordinate
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.topic.delete",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.delete", coordinate),
			})
			if err != nil {
				return err
			}
			operationErr := backend.DeleteTopic(cmd.Context(), coordinate)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "DeleteResult", map[string]string{"topic": topic, "status": audit.StatusSuccess}, opTarget, operationTargetWrite)
		},
	}
}

func newTopicPurgeCmd(f *cliFlags) *cobra.Command {
	var dlq bool
	cmd := &cobra.Command{
		Use:   "purge TOPIC",
		Short: "Purge topic or DLQ messages",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsPartitions(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support topic purge", nil)
			}
			topic := args[0]
			op := mqclass.OperationPurgeTopic
			if dlq {
				op = mqclass.OperationPurgeDLQ
			}
			dryRun := f.DryRun || f.Plan
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, dryRun)
			if err != nil {
				return err
			}
			allow := allowTopicPurge
			if dryRun {
				allow = ""
			}
			if err := classifyAndAuthorize(f, meta, op, resolved.Classification, allow); err != nil {
				return err
			}
			request := mqgov.TopicPurgeRequest{Coordinate: resolved.Coordinate, DLQ: dlq, DryRun: dryRun}
			if dryRun {
				result, err := manager.PurgeTopic(cmd.Context(), request)
				if err != nil {
					appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusFailed, "purge plan", err)
					return err
				}
				appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusSuccess, fmt.Sprintf("purge plan count=%d", result.Fingerprint.Count), nil)
				return targetJSONData(f, "TopicPurgeResult", result, opTarget, operationTargetWrite)
			}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.topic.purge",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "topic", Resource: topic},
				Metadata: mutationValueMetadata("mq.topic.purge", request),
			})
			if err != nil {
				return err
			}
			result, operationErr := manager.PurgeTopic(cmd.Context(), request)
			failed := 0
			if operationErr != nil {
				failed = 1
			}
			total := int(result.Fingerprint.Count) + failed
			if err := finishBatchMutationAudit(handle, total, int(result.Fingerprint.Count), failed, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "TopicPurgeResult", result, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().BoolVar(&dlq, "dlq", false, "Purge DLQ")
	return cmd
}

func topicCoord(f *cliFlags, meta mqgovctx.Context, topic string) mqgov.TopicCoordinate {
	return mqgov.TopicCoordinate{Cluster: firstNonEmpty(f.Cluster, meta.Cluster, "fake"), Namespace: firstNonEmpty(f.Namespace, meta.Namespace), Topic: topic}
}
