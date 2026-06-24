package cmd

import (
	"fmt"
	"strconv"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/audit"
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
				return newPrinter(f).JSONList("DLQList", items, len(items), 1, len(items), false)
			}
			rows := make([][]string, 0, len(items))
			for _, item := range items {
				rows = append(rows, []string{item.Coordinate.Topic, item.SourceTopic, item.ConsumerGroup, item.NativeModel, strconv.FormatInt(item.Messages, 10)})
			}
			newPrinter(f).Table([]string{"DLQ", "SOURCE", "GROUP", "NATIVE MODEL", "MESSAGES"}, rows)
			return nil
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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			if !backend.Capabilities().SupportsDLQPeek {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support non-destructive DLQ peek", nil)
			}
			manager, ok := mqgov.SupportsDLQ(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
			}
			dlq := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationPeekDLQ, mqclass.Target{Topic: dlq, Group: group}, ""); err != nil {
				return err
			}
			result, err := manager.PeekDLQ(cmd.Context(), mqgov.DLQPeekRequest{DLQ: topicCoord(f, meta, dlq), Topic: topic, Group: group, Partition: partition, Offset: offset, Count: count})
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusSuccess, fmt.Sprintf("peek count=%d", result.Count), nil)
			return newPrinter(f).JSONData("DLQPeekResult", result)
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
			dryRun := f.DryRun || f.Plan
			allow := allowInternalProduce
			if dryRun {
				allow = ""
			}
			classTarget := mqclass.Target{Topic: target, Group: group, Plan: dryRun}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationRedriveDLQ, classTarget, allow); err != nil {
				return err
			}
			result, err := manager.RedriveDLQ(cmd.Context(), mqgov.DLQRedriveRequest{DLQ: topicCoord(f, meta, dlq), Target: topicCoord(f, meta, target), Topic: topic, Group: group, Count: count, DryRun: dryRun})
			if err != nil {
				appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusFailed, "redrive", err)
				return err
			}
			appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusSuccess, fmt.Sprintf("redrive count=%d", result.Fingerprint.Count), nil)
			return newPrinter(f).JSONData("DLQRedriveResult", result)
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
			if !backend.Capabilities().SupportsDLQPurge {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ purge", nil)
			}
			manager, ok := mqgov.SupportsDLQ(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
			}
			dlq := args[0]
			dryRun := f.DryRun || f.Plan
			allow := allowTopicPurge
			if dryRun {
				allow = ""
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationPurgeDLQ, mqclass.Target{Topic: dlq, Group: group, Plan: dryRun}, allow); err != nil {
				return err
			}
			result, err := manager.PurgeDLQ(cmd.Context(), mqgov.DLQPurgeRequest{DLQ: topicCoord(f, meta, dlq), Topic: topic, Group: group, DryRun: dryRun})
			if err != nil {
				appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusFailed, "purge", err)
				return err
			}
			appendAuditWarn(f, auditEventDLQ, meta, audit.EventTarget{ResourceType: "dlq", Resource: dlq}, audit.StatusSuccess, fmt.Sprintf("purge count=%d", result.Fingerprint.Count), nil)
			return newPrinter(f).JSONData("DLQPurgeResult", result)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Source topic hint")
	cmd.Flags().StringVar(&group, "group", "", "Consumer group or subscription hint")
	return cmd
}
