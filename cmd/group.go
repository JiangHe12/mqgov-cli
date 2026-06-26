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
			targetTable(f, []string{"GROUP", "MEMBERS", "STATE"}, rows, opTarget)
			return nil
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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			opTarget := operationTargetFromBroker(f, backend)
			group := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationCreateGroup, mqclass.Target{Group: group}, ""); err != nil {
				return err
			}
			desc, err := backend.CreateGroup(cmd.Context(), groupCoord(f, meta, group))
			if err != nil {
				appendAuditWarn(f, auditEventGroup, meta, audit.EventTarget{ResourceType: "group", Resource: group}, audit.StatusFailed, "create", err)
				return err
			}
			appendAuditWarn(f, auditEventGroup, meta, audit.EventTarget{ResourceType: "group", Resource: group}, audit.StatusSuccess, "create", nil)
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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			opTarget := operationTargetFromBroker(f, backend)
			group := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationDeleteGroup, mqclass.Target{Group: group}, ""); err != nil {
				return err
			}
			if err := backend.DeleteGroup(cmd.Context(), groupCoord(f, meta, group)); err != nil {
				appendAuditWarn(f, auditEventGroup, meta, audit.EventTarget{ResourceType: "group", Resource: group}, audit.StatusFailed, "delete", err)
				return err
			}
			appendAuditWarn(f, auditEventGroup, meta, audit.EventTarget{ResourceType: "group", Resource: group}, audit.StatusSuccess, "delete", nil)
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
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsOffsets(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support offsets", nil)
			}
			group, topic := args[0], args[1]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationLag, mqclass.Target{Group: group, Topic: topic}, ""); err != nil {
				return err
			}
			plan, err := manager.Lag(cmd.Context(), groupCoord(f, meta, group), topicCoord(f, meta, topic))
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
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsOffsets(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support offsets", nil)
			}
			group, topic := args[0], args[1]
			dryRun := f.DryRun || f.Plan
			allow := allowOffsetReset
			if dryRun {
				allow = ""
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationResetOffset, mqclass.Target{Group: group, Topic: topic, Plan: dryRun}, allow); err != nil {
				return err
			}
			req := mqgov.OffsetPlanRequest{Group: groupCoord(f, meta, group), Topic: topicCoord(f, meta, topic), To: to, DryRun: dryRun, Partition: -1}
			var plan mqgov.OffsetPlan
			if req.DryRun {
				plan, err = manager.PlanOffsetReset(cmd.Context(), req)
			} else {
				plan, err = manager.ResetOffset(cmd.Context(), req)
			}
			if err != nil {
				appendAuditWarn(f, auditEventOffset, meta, audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic}, audit.StatusFailed, "reset-offset", err)
				return err
			}
			appendAuditWarn(f, auditEventOffset, meta, audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic}, audit.StatusSuccess, fmt.Sprintf("reset-offset count=%d", plan.Total), nil)
			return targetJSONData(f, "OffsetPlan", plan, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&to, "to", "earliest", "Target offset: earliest | latest | offset:N | datetime:<RFC3339> | shift:±N (some targets are backend-specific and unsupported backends return clear errors)")
	return cmd
}

func groupCoord(f *cliFlags, meta mqgovctx.Context, group string) mqgov.GroupCoordinate {
	return mqgov.GroupCoordinate{Cluster: firstNonEmpty(f.Cluster, meta.Cluster, "fake"), Namespace: firstNonEmpty(f.Namespace, meta.Namespace), Group: group}
}
