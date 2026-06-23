package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/audit"

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
			if err := classifyAndAuthorize(f, meta, mqclass.OperationList, mqclass.Target{Topic: pattern}, ""); err != nil {
				return err
			}
			items, err := backend.ListTopics(cmd.Context(), mqgov.TopicListOptions{Namespace: f.Namespace, Pattern: pattern})
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic"}, audit.StatusSuccess, fmt.Sprintf("list count=%d", len(items)), nil)
			if f.Output == "json" {
				return newPrinter(f).JSONList("TopicList", items, len(items), 1, len(items), false)
			}
			rows := make([][]string, 0, len(items))
			for _, item := range items {
				rows = append(rows, []string{item.Coordinate.Topic, strconv.Itoa(item.Partitions), strconv.FormatBool(item.Protected), strconv.FormatBool(item.Internal)})
			}
			newPrinter(f).Table([]string{"TOPIC", "PARTITIONS", "PROTECTED", "INTERNAL"}, rows)
			return nil
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
			topic := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationDescribe, mqclass.Target{Topic: topic}, ""); err != nil {
				return err
			}
			desc, err := backend.DescribeTopic(cmd.Context(), topicCoord(f, meta, topic))
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusSuccess, "describe", nil)
			return newPrinter(f).JSONData("TopicDescription", desc)
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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			topic := args[0]
			target := mqclass.Target{Topic: topic, ProtectedTopic: isProtectedTopic(meta, topic, mqgov.TopicDescription{})}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationCreateTopic, target, ""); err != nil {
				return err
			}
			desc, err := backend.CreateTopic(cmd.Context(), mqgov.TopicCreateRequest{Coordinate: topicCoord(f, meta, topic), Partitions: partitions, Protected: target.ProtectedTopic})
			if err != nil {
				appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusFailed, "create", err)
				return err
			}
			appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusSuccess, "create", nil)
			return newPrinter(f).JSONData("TopicDescription", desc)
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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			manager, ok := mqgov.SupportsPartitions(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support partition management", nil)
			}
			topic := args[0]
			desc, _ := backend.DescribeTopic(cmd.Context(), topicCoord(f, meta, topic))
			target := mqclass.Target{Topic: topic, ProtectedTopic: isProtectedTopic(meta, topic, desc), InternalTopic: desc.Internal}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationAlterTopic, target, ""); err != nil {
				return err
			}
			changed, err := manager.AlterTopic(cmd.Context(), mqgov.TopicAlterRequest{Coordinate: topicCoord(f, meta, topic), Partitions: partitions})
			if err != nil {
				appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusFailed, "alter", err)
				return err
			}
			appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusSuccess, "alter", nil)
			return newPrinter(f).JSONData("TopicDescription", changed)
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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			topic := args[0]
			desc, _ := backend.DescribeTopic(cmd.Context(), topicCoord(f, meta, topic))
			target := mqclass.Target{Topic: topic, ProtectedTopic: isProtectedTopic(meta, topic, desc), InternalTopic: desc.Internal}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationDeleteTopic, target, allowTopicDelete); err != nil {
				return err
			}
			if err := backend.DeleteTopic(cmd.Context(), topicCoord(f, meta, topic)); err != nil {
				appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusFailed, "delete", err)
				return err
			}
			appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusSuccess, "delete", nil)
			return newPrinter(f).JSONData("DeleteResult", map[string]string{"topic": topic, "status": audit.StatusSuccess})
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
			manager, ok := mqgov.SupportsPartitions(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support topic purge", nil)
			}
			topic := args[0]
			desc, _ := backend.DescribeTopic(cmd.Context(), topicCoord(f, meta, topic))
			op := mqclass.OperationPurgeTopic
			if dlq {
				op = mqclass.OperationPurgeDLQ
			}
			dryRun := f.DryRun || f.Plan
			allow := allowTopicPurge
			if dryRun {
				allow = ""
			}
			if err := classifyAndAuthorize(f, meta, op, mqclass.Target{Topic: topic, ProtectedTopic: isProtectedTopic(meta, topic, desc), InternalTopic: desc.Internal, Plan: dryRun}, allow); err != nil {
				return err
			}
			result, err := manager.PurgeTopic(cmd.Context(), mqgov.TopicPurgeRequest{Coordinate: topicCoord(f, meta, topic), DLQ: dlq, DryRun: dryRun})
			if err != nil {
				appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusFailed, "purge", err)
				return err
			}
			appendAuditWarn(f, auditEventTopic, meta, audit.EventTarget{ResourceType: "topic", Resource: topic}, audit.StatusSuccess, fmt.Sprintf("purge count=%d", result.Fingerprint.Count), nil)
			return newPrinter(f).JSONData("TopicPurgeResult", result)
		},
	}
	cmd.Flags().BoolVar(&dlq, "dlq", false, "Purge DLQ")
	return cmd
}

func topicCoord(f *cliFlags, meta mqgovctx.Context, topic string) mqgov.TopicCoordinate {
	return mqgov.TopicCoordinate{Cluster: firstNonEmpty(f.Cluster, meta.Cluster, "fake"), Namespace: firstNonEmpty(f.Namespace, meta.Namespace), Topic: topic}
}
