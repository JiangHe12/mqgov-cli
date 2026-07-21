package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/spf13/cobra"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func newDLQCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "dlq", Short: "Govern dead-letter queues"}
	cmd.AddCommand(newDLQListCmd(f), newDLQPeekCmd(f), newDLQRedriveCmd(f), newDLQPurgeCmd(f))
	return cmd
}

func newDLQListCmd(f *cliFlags) *cobra.Command {
	var topic string
	var group string
	var pattern string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List native DLQs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			if !backend.Capabilities().SupportsDLQList {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ listing", nil)
			}
			manager, ok := mqgov.SupportsDLQ(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationListDLQ, mqclass.Target{Topic: firstNonEmpty(topic, pattern), Group: group}, ""); err != nil {
				return err
			}
			items, err := manager.ListDLQs(cmd.Context(), mqgov.DLQListOptions{Namespace: f.Namespace, Topic: topic, Group: group, Pattern: pattern})
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq"}, audit.StatusSuccess, fmt.Sprintf("list count=%d", len(items)), nil)
			if f.Output == "json" {
				return targetJSONList(f, "DLQList", items, len(items), len(items), opTarget)
			}
			rows := make([][]string, 0, len(items))
			for _, item := range items {
				rows = append(rows, []string{item.Coordinate.Topic, item.SourceTopic, item.ConsumerGroup, item.NativeModel, strconv.FormatInt(item.Messages, 10)})
			}
			return targetTable(f, []string{"DLQ", "SOURCE", "GROUP", "NATIVE MODEL", "MESSAGES"}, rows, opTarget)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Source topic or explicit Kafka DLQ topic")
	cmd.Flags().StringVar(&group, "group", "", "Consumer group or subscription")
	cmd.Flags().StringVar(&pattern, "pattern", "", "Exact DLQ name or wildcard pattern")
	return cmd
}

func newDLQPeekCmd(f *cliFlags) *cobra.Command {
	var topic string
	var group string
	var partition int
	var offset int64
	var count int
	cmd := &cobra.Command{
		Use:   "peek DLQ",
		Short: "Peek DLQ message fingerprints without bodies",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validatePeekWindow(partition, offset, count); err != nil {
				return err
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			if !backend.Capabilities().SupportsDLQPeek {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support non-destructive DLQ peek", nil)
			}
			manager, ok := mqgov.SupportsDLQ(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
			}
			dlq := args[0]
			resolvedDLQ, err := resolveTopicTarget(cmd.Context(), backend, f, meta, effectiveDLQTopic(backend, dlq, topic, group), false)
			if err != nil {
				return err
			}
			classTarget := resolvedDLQ.Classification
			classTarget.Group = group
			if err := classifyAndAuthorize(f, meta, mqclass.OperationPeekDLQ, classTarget, ""); err != nil {
				return err
			}
			result, err := manager.PeekDLQ(cmd.Context(), mqgov.DLQPeekRequest{DLQ: resolvedDLQ.Coordinate, Topic: topic, Group: group, Partition: partition, Offset: offset, Count: count})
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusSuccess, fmt.Sprintf("peek count=%d", result.Count), nil)
			return targetJSONData(f, "DLQPeekResult", result, opTarget, operationTargetRead)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Source topic hint")
	cmd.Flags().StringVar(&group, "group", "", "Consumer group or subscription hint")
	cmd.Flags().IntVar(&partition, "partition", 0, "Partition")
	cmd.Flags().Int64Var(&offset, "offset", 0, "Offset")
	cmd.Flags().IntVar(&count, "count", 1, "Maximum messages")
	return cmd
}

func newDLQRedriveCmd(f *cliFlags) *cobra.Command {
	var topic string
	var group string
	var target string
	var count int
	cmd := &cobra.Command{
		Use:   "redrive DLQ",
		Short: "Redrive DLQ messages to a live non-DLQ topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			if !backend.Capabilities().SupportsDLQRedrive {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ redrive", nil)
			}
			manager, ok := mqgov.SupportsDLQ(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
			}
			dlq := args[0]
			if target == "" {
				return apperrors.New(apperrors.CodeUsageError, "redrive target topic is required", nil)
			}
			if count <= 0 {
				return apperrors.New(apperrors.CodeUsageError, "redrive count must be positive", nil)
			}
			dlqTopic := effectiveDLQTopic(backend, dlq, topic, group)
			if target == dlqTopic {
				return apperrors.New(apperrors.CodeUsageError, "redrive target must differ from the DLQ", nil)
			}
			dryRun := f.DryRun || f.Plan
			resolvedDLQ, err := resolveTopicTarget(cmd.Context(), backend, f, meta, dlqTopic, dryRun)
			if err != nil {
				return err
			}
			resolvedTarget, err := resolveTopicTarget(cmd.Context(), backend, f, meta, target, dryRun)
			if err != nil {
				return err
			}
			allow := allowInternalProduce
			if dryRun {
				allow = ""
			}
			classTarget := resolvedTarget.Classification
			classTarget.Group = group
			if err := classifyAndAuthorize(f, meta, mqclass.OperationRedriveDLQ, classTarget, allow); err != nil {
				return err
			}
			request := mqgov.DLQRedriveRequest{DLQ: resolvedDLQ.Coordinate, Target: resolvedTarget.Coordinate, Topic: topic, Group: group, Count: count, DryRun: dryRun}
			if dryRun {
				result, err := manager.RedriveDLQ(cmd.Context(), request)
				if err != nil {
					appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusFailed, "redrive plan", err)
					return err
				}
				appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusSuccess, fmt.Sprintf("redrive plan count=%d", result.Fingerprint.Count), nil)
				return targetJSONData(f, "DLQRedriveResult", result, opTarget, operationTargetWrite)
			}
			metadata := mutationValueMetadata("mq.dlq.redrive", request)
			metadata.Items = count
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.dlq.redrive",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "dlq", Resource: dlq},
				Metadata: metadata,
			})
			if err != nil {
				return err
			}
			result, operationErr := manager.RedriveDLQ(cmd.Context(), request)
			failed := 0
			if operationErr != nil {
				failed = 1
			}
			succeeded := int(result.Fingerprint.Count)
			if err := finishBatchMutationAudit(handle, succeeded+failed, succeeded, failed, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "DLQRedriveResult", result, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Source topic hint")
	cmd.Flags().StringVar(&group, "group", "", "Consumer group or subscription hint")
	cmd.Flags().StringVar(&target, "target", "", "Live non-DLQ target topic")
	cmd.Flags().IntVar(&count, "count", 100, "Maximum messages to redrive or preview")
	return cmd
}

func newDLQPurgeCmd(f *cliFlags) *cobra.Command {
	var topic string
	var group string
	cmd := &cobra.Command{
		Use:   "purge DLQ",
		Short: "Purge DLQ messages",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			if !backend.Capabilities().SupportsDLQPurge {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ purge", nil)
			}
			manager, ok := mqgov.SupportsDLQ(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
			}
			dlq := args[0]
			dryRun := f.DryRun || f.Plan
			resolvedDLQ, err := resolveTopicTarget(cmd.Context(), backend, f, meta, effectiveDLQTopic(backend, dlq, topic, group), dryRun)
			if err != nil {
				return err
			}
			allow := allowTopicPurge
			if dryRun {
				allow = ""
			}
			classTarget := resolvedDLQ.Classification
			classTarget.Group = group
			if err := classifyAndAuthorize(f, meta, mqclass.OperationPurgeDLQ, classTarget, allow); err != nil {
				return err
			}
			request := mqgov.DLQPurgeRequest{DLQ: resolvedDLQ.Coordinate, Topic: topic, Group: group, DryRun: dryRun}
			if dryRun {
				result, err := manager.PurgeDLQ(cmd.Context(), request)
				if err != nil {
					appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusFailed, "purge plan", err)
					return err
				}
				appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusSuccess, fmt.Sprintf("purge plan count=%d", result.Fingerprint.Count), nil)
				return targetJSONData(f, "DLQPurgeResult", result, opTarget, operationTargetWrite)
			}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.dlq.purge",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "dlq", Resource: dlq},
				Metadata: mutationValueMetadata("mq.dlq.purge", request),
			})
			if err != nil {
				return err
			}
			result, operationErr := manager.PurgeDLQ(cmd.Context(), request)
			failed := 0
			if operationErr != nil {
				failed = 1
			}
			succeeded := int(result.Fingerprint.Count)
			if err := finishBatchMutationAudit(handle, succeeded+failed, succeeded, failed, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "DLQPurgeResult", result, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Source topic hint")
	cmd.Flags().StringVar(&group, "group", "", "Consumer group or subscription hint")
	return cmd
}

func effectiveDLQTopic(backend mqgov.Broker, explicit, topic, group string) string {
	if backend.Describe().Backend != "pulsar" || topic == "" || group == "" {
		return explicit
	}
	name := strings.TrimSpace(topic)
	if index := strings.LastIndex(name, "/"); index >= 0 {
		name = name[index+1:]
	}
	return name + "-" + group + "-DLQ"
}
