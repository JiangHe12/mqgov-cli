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
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
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
			if err := validateExactPattern(pattern); err != nil {
				return err
			}
			options := mqgov.DLQListOptions{Namespace: f.Namespace, Topic: topic, Group: group, Pattern: pattern}
			items, opTarget, err := runMandatoryBrokerRead(f, readAuditSpec{
				Action:   "mq.dlq.list",
				Target:   audit.EventTarget{ResourceType: "dlq"},
				Metadata: mutationValueMetadata("mq.dlq.list", options),
			}, func(meta mqgovctx.Context) error {
				return classifyAndAuthorize(f, meta, mqclass.OperationListDLQ, mqclass.Target{Topic: firstNonEmpty(topic, pattern), Group: group}, "")
			}, func(backend mqgov.Broker, meta mqgovctx.Context) ([]mqgov.DLQDescription, error) {
				if !backend.Capabilities().SupportsDLQList {
					return nil, apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ listing", nil)
				}
				manager, ok := mqgov.SupportsDLQ(backend)
				if !ok {
					return nil, apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
				}
				return manager.ListDLQs(cmd.Context(), options)
			}, func(items []mqgov.DLQDescription) int {
				return len(items)
			})
			if err != nil {
				return err
			}
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
	cmd.Flags().StringVar(&pattern, "pattern", "", "Exact DLQ name")
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
			dlq := args[0]
			result, opTarget, err := runMandatoryBrokerRead(f, readAuditSpec{
				Action: "mq.dlq.peek",
				Target: audit.EventTarget{ResourceType: "dlq", Resource: dlq},
				Metadata: mutationValueMetadata("mq.dlq.peek", struct {
					DLQ       string
					Topic     string
					Group     string
					Partition int
					Offset    int64
					Count     int
				}{
					DLQ:       dlq,
					Topic:     topic,
					Group:     group,
					Partition: partition,
					Offset:    offset,
					Count:     count,
				}),
			}, func(meta mqgovctx.Context) error {
				target := declaredTopicTarget(meta, firstNonEmpty(f.Backend, meta.Backend, defaultFakeBackend), firstNonEmpty(topic, dlq), false)
				target.Group = group
				return classifyAndAuthorize(f, meta, mqclass.OperationPeekDLQ, target, "")
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (mqgov.DLQPeekResult, error) {
				if !backend.Capabilities().SupportsDLQPeek {
					return mqgov.DLQPeekResult{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support non-destructive DLQ peek", nil)
				}
				manager, ok := mqgov.SupportsDLQ(backend)
				if !ok {
					return mqgov.DLQPeekResult{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
				}
				resolvedDLQ, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, effectiveDLQTopic(backend, dlq, topic, group), false)
				if resolveErr != nil {
					return mqgov.DLQPeekResult{}, resolveErr
				}
				classTarget := resolvedDLQ.Classification
				classTarget.Group = group
				if authorizeErr := classifyAndAuthorize(f, meta, mqclass.OperationPeekDLQ, classTarget, ""); authorizeErr != nil {
					return mqgov.DLQPeekResult{}, authorizeErr
				}
				return manager.PeekDLQ(cmd.Context(), mqgov.DLQPeekRequest{DLQ: resolvedDLQ.Coordinate, Topic: topic, Group: group, Partition: partition, Offset: offset, Count: count})
			}, func(result mqgov.DLQPeekResult) int {
				return result.Count
			})
			if err != nil {
				return err
			}
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
			if target == "" {
				return apperrors.New(apperrors.CodeUsageError, "redrive target topic is required", nil)
			}
			if count <= 0 || count > mqgov.MaxMessageBatchSize {
				return apperrors.New(
					apperrors.CodeUsageError,
					fmt.Sprintf("redrive count must be between 1 and %d", mqgov.MaxMessageBatchSize),
					nil,
				)
			}
			dlq := args[0]
			dryRun := f.DryRun || f.Plan
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.dlq.redrive.preflight",
				Target:   audit.EventTarget{ResourceType: "dlq", Resource: dlq},
				Metadata: mutationValueMetadata("mq.dlq.redrive.preflight", map[string]any{"dlq": dlq, "target": target, "count": count, "dryRun": dryRun}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (dlqRedrivePreflight, error) {
				if !backend.Capabilities().SupportsDLQRedrive {
					return dlqRedrivePreflight{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ redrive", nil)
				}
				manager, ok := mqgov.SupportsDLQ(backend)
				if !ok {
					return dlqRedrivePreflight{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
				}
				dlqTopic := effectiveDLQTopic(backend, dlq, topic, group)
				if target == dlqTopic {
					return dlqRedrivePreflight{}, apperrors.New(apperrors.CodeUsageError, "redrive target must differ from the DLQ", nil)
				}
				resolvedDLQ, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, dlqTopic, dryRun)
				if resolveErr != nil {
					return dlqRedrivePreflight{}, resolveErr
				}
				resolvedTarget, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, target, dryRun)
				if resolveErr != nil {
					return dlqRedrivePreflight{}, resolveErr
				}
				request := mqgov.DLQRedriveRequest{DLQ: resolvedDLQ.Coordinate, Target: resolvedTarget.Coordinate, Topic: topic, Group: group, Count: count, DryRun: dryRun}
				value := dlqRedrivePreflight{Manager: manager, Target: resolvedTarget, Request: request}
				if !dryRun {
					return value, nil
				}
				classTarget := resolvedTarget.Classification
				classTarget.Group = group
				if authorizeErr := classifyAndAuthorize(f, meta, mqclass.OperationRedriveDLQ, classTarget, ""); authorizeErr != nil {
					return dlqRedrivePreflight{}, authorizeErr
				}
				value.Preview, resolveErr = manager.RedriveDLQ(cmd.Context(), request)
				value.HasPreview = resolveErr == nil
				return value, resolveErr
			}, func(value dlqRedrivePreflight) int {
				if value.HasPreview {
					return int(value.Preview.Fingerprint.Count)
				}
				return 1
			})
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			allow := allowInternalProduce
			if dryRun {
				allow = ""
			}
			classTarget := preflight.Value.Target.Classification
			classTarget.Group = group
			if !dryRun {
				if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationRedriveDLQ, classTarget, allow); err != nil {
					return err
				}
			}
			request := preflight.Value.Request
			if dryRun {
				return targetJSONData(f, "DLQRedriveResult", preflight.Value.Preview, preflight.Target, operationTargetWrite)
			}
			metadata := mutationValueMetadata("mq.dlq.redrive", request)
			metadata.Items = count
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.dlq.redrive",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "dlq", Resource: dlq},
				Metadata: metadata,
			})
			if err != nil {
				return err
			}
			result, operationErr := preflight.Value.Manager.RedriveDLQ(cmd.Context(), request)
			outcome := result.BatchOutcome
			if outcome.Empty() {
				outcome.Succeeded = int(result.Fingerprint.Count)
				if operationErr != nil {
					outcome.Failed = 1
				}
			}
			if err := finishBatchMutationAuditWithOutcome(handle, count, outcome, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "DLQRedriveResult", result, preflight.Target, operationTargetWrite)
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
			dlq := args[0]
			dryRun := f.DryRun || f.Plan
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.dlq.purge.preflight",
				Target:   audit.EventTarget{ResourceType: "dlq", Resource: dlq},
				Metadata: mutationValueMetadata("mq.dlq.purge.preflight", map[string]any{"dlq": dlq, "dryRun": dryRun}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (dlqPurgePreflight, error) {
				if !backend.Capabilities().SupportsDLQPurge {
					return dlqPurgePreflight{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ purge", nil)
				}
				manager, ok := mqgov.SupportsDLQ(backend)
				if !ok {
					return dlqPurgePreflight{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support DLQ governance", nil)
				}
				resolved, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, effectiveDLQTopic(backend, dlq, topic, group), dryRun)
				if resolveErr != nil {
					return dlqPurgePreflight{}, resolveErr
				}
				request := mqgov.DLQPurgeRequest{DLQ: resolved.Coordinate, Topic: topic, Group: group, DryRun: dryRun}
				value := dlqPurgePreflight{Manager: manager, Resolved: resolved, Request: request}
				if !dryRun {
					return value, nil
				}
				classTarget := resolved.Classification
				classTarget.Group = group
				if authorizeErr := classifyAndAuthorize(f, meta, mqclass.OperationPurgeDLQ, classTarget, ""); authorizeErr != nil {
					return dlqPurgePreflight{}, authorizeErr
				}
				value.Preview, resolveErr = manager.PurgeDLQ(cmd.Context(), request)
				value.HasPreview = resolveErr == nil
				return value, resolveErr
			}, func(value dlqPurgePreflight) int {
				if value.HasPreview {
					return int(value.Preview.Fingerprint.Count)
				}
				return 1
			})
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			allow := allowTopicPurge
			if dryRun {
				allow = ""
			}
			classTarget := preflight.Value.Resolved.Classification
			classTarget.Group = group
			if !dryRun {
				if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationPurgeDLQ, classTarget, allow); err != nil {
					return err
				}
			}
			request := preflight.Value.Request
			if dryRun {
				return targetJSONData(f, "DLQPurgeResult", preflight.Value.Preview, preflight.Target, operationTargetWrite)
			}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.dlq.purge",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "dlq", Resource: dlq},
				Metadata: mutationValueMetadata("mq.dlq.purge", request),
			})
			if err != nil {
				return err
			}
			result, operationErr := preflight.Value.Manager.PurgeDLQ(cmd.Context(), request)
			failed := 0
			if operationErr != nil {
				failed = 1
			}
			succeeded := int(result.Fingerprint.Count)
			if err := finishBatchMutationAudit(handle, succeeded+failed, succeeded, failed, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "DLQPurgeResult", result, preflight.Target, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Source topic hint")
	cmd.Flags().StringVar(&group, "group", "", "Consumer group or subscription hint")
	return cmd
}

type dlqRedrivePreflight struct {
	Manager    mqgov.DLQManager
	Target     resolvedTopicTarget
	Request    mqgov.DLQRedriveRequest
	Preview    mqgov.DLQRedriveResult
	HasPreview bool
}

type dlqPurgePreflight struct {
	Manager    mqgov.DLQManager
	Resolved   resolvedTopicTarget
	Request    mqgov.DLQPurgeRequest
	Preview    mqgov.DLQPurgeResult
	HasPreview bool
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
