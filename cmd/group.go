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

func newGroupCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "group", Short: "Manage consumer groups"}
	cmd.AddCommand(newGroupListCmd(f), newGroupCreateCmd(f), newGroupDeleteCmd(f), newGroupLagCmd(f), newGroupResetOffsetCmd(f))
	return cmd
}

func newGroupListCmd(f *cliFlags) *cobra.Command {
	var pattern string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List groups",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			if err := classifyAndAuthorize(f, meta, mqclass.OperationList, mqclass.Target{Group: pattern}, ""); err != nil {
				return err
			}
			items, err := backend.ListGroups(cmd.Context(), mqgov.GroupListOptions{Namespace: f.Namespace, Pattern: pattern})
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventGroup, meta, audit.EventTarget{ResourceType: "group"}, audit.StatusSuccess, fmt.Sprintf("list count=%d", len(items)), nil)
			if f.Output == "json" {
				return targetJSONList(f, "GroupList", items, len(items), len(items), opTarget)
			}
			rows := make([][]string, 0, len(items))
			for _, item := range items {
				rows = append(rows, []string{item.Coordinate.Group, strconv.Itoa(item.Members), item.State})
			}
			return targetTable(f, []string{"GROUP", "MEMBERS", "STATE"}, rows, opTarget)
		},
	}
	cmd.Flags().StringVar(&pattern, "pattern", "", "Exact group name or wildcard pattern")
	return cmd
}

func newGroupCreateCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "create GROUP",
		Short: "Create a group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "create", "group", args[0], nil)
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			group := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationCreateGroup, mqclass.Target{Group: group}, ""); err != nil {
				return err
			}
			coordinate := groupCoord(f, meta, group)
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.group.create",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "group", Resource: group},
				Metadata: mutationValueMetadata("mq.group.create", coordinate),
			})
			if err != nil {
				return err
			}
			desc, operationErr := backend.CreateGroup(cmd.Context(), coordinate)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "GroupDescription", desc, opTarget, operationTargetWrite)
		},
	}
}

func newGroupDeleteCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete GROUP",
		Short: "Delete a group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "delete", "group", args[0], nil)
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			group := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationDeleteGroup, mqclass.Target{Group: group}, ""); err != nil {
				return err
			}
			coordinate := groupCoord(f, meta, group)
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.group.delete",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "group", Resource: group},
				Metadata: mutationValueMetadata("mq.group.delete", coordinate),
			})
			if err != nil {
				return err
			}
			operationErr := backend.DeleteGroup(cmd.Context(), coordinate)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "DeleteResult", map[string]string{"group": group, "status": audit.StatusSuccess}, opTarget, operationTargetWrite)
		},
	}
}

func newGroupLagCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "lag GROUP TOPIC",
		Short: "Show group lag",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsOffsets(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support offsets", nil)
			}
			group, topic := args[0], args[1]
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			if err != nil {
				return err
			}
			target := resolved.Classification
			target.Group = group
			if err := classifyAndAuthorize(f, meta, mqclass.OperationLag, target, ""); err != nil {
				return err
			}
			plan, err := manager.Lag(cmd.Context(), groupCoord(f, meta, group), resolved.Coordinate)
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventOffset, meta, audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic}, audit.StatusSuccess, fmt.Sprintf("lag count=%d", plan.Total), nil)
			return targetJSONData(f, "LagResult", plan, opTarget, operationTargetRead)
		},
	}
}

func newGroupResetOffsetCmd(f *cliFlags) *cobra.Command {
	var to string
	cmd := &cobra.Command{
		Use:   "reset-offset GROUP TOPIC",
		Short: "Plan or reset group offsets",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsOffsets(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support offsets", nil)
			}
			group, topic := args[0], args[1]
			dryRun := f.DryRun || f.Plan
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, dryRun)
			if err != nil {
				return err
			}
			allow := allowOffsetReset
			if dryRun {
				allow = ""
			}
			target := resolved.Classification
			target.Group = group
			if err := classifyAndAuthorize(f, meta, mqclass.OperationResetOffset, target, allow); err != nil {
				return err
			}
			req := mqgov.OffsetPlanRequest{Group: groupCoord(f, meta, group), Topic: resolved.Coordinate, To: to, DryRun: dryRun, Partition: -1}
			if req.DryRun {
				plan, planErr := manager.PlanOffsetReset(cmd.Context(), req)
				if planErr != nil {
					appendAuditWarn(f, auditEventOffset, meta, audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic}, audit.StatusFailed, "reset-offset", planErr)
					return planErr
				}
				appendAuditWarn(f, auditEventOffset, meta, audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic}, audit.StatusSuccess, fmt.Sprintf("reset-offset plan count=%d", plan.Total), nil)
				return targetJSONData(f, "OffsetPlan", plan, opTarget, operationTargetWrite)
			}
			previewRequest := req
			previewRequest.DryRun = true
			preview, previewErr := manager.PlanOffsetReset(cmd.Context(), previewRequest)
			if previewErr != nil {
				appendAuditWarn(f, auditEventOffset, meta, audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic}, audit.StatusFailed, "reset-offset preflight", previewErr)
				return previewErr
			}
			handle, auditErr := beginMutationAudit(f, mutationAuditSpec{
				Action:  "mq.offset.reset",
				Context: meta,
				Target:  audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic},
				Metadata: mutationValueMetadata("mq.offset.reset", struct {
					Request mqgov.OffsetPlanRequest
					Preview mqgov.OffsetPlan
				}{Request: req, Preview: preview}),
			})
			if auditErr != nil {
				return auditErr
			}
			plan, operationErr := manager.ResetOffset(cmd.Context(), req)
			failed := 0
			if operationErr != nil {
				failed = 1
			}
			total := int(plan.Total) + failed
			if auditErr := finishBatchMutationAudit(handle, total, int(plan.Total), failed, operationErr); auditErr != nil {
				return auditErr
			}
			return targetJSONData(f, "OffsetPlan", plan, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&to, "to", "earliest", "Target offset: earliest | latest | offset:N | datetime:<RFC3339> | shift:±N (some targets are backend-specific and unsupported backends return clear errors)")
	return cmd
}

func groupCoord(f *cliFlags, meta mqgovctx.Context, group string) mqgov.GroupCoordinate {
	return mqgov.GroupCoordinate{Cluster: firstNonEmpty(f.Cluster, meta.Cluster, "fake"), Namespace: firstNonEmpty(f.Namespace, meta.Namespace), Group: group}
}
