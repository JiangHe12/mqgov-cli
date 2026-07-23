package cmd

import (
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
			if err := validateExactPattern(pattern); err != nil {
				return err
			}
			options := mqgov.GroupListOptions{Namespace: f.Namespace, Pattern: pattern}
			items, opTarget, err := runMandatoryBrokerList(f, readAuditSpec{
				Action:   "mq.group.list",
				Target:   audit.EventTarget{ResourceType: "group"},
				Metadata: mutationValueMetadata("mq.group.list", options),
			}, func(meta mqgovctx.Context) error {
				return classifyAndAuthorize(f, meta, mqclass.OperationList, mqclass.Target{Group: pattern}, "")
			}, func(backend mqgov.Broker, _ mqgovctx.Context) ([]mqgov.GroupDescription, error) {
				return backend.ListGroups(cmd.Context(), options)
			})
			if err != nil {
				return err
			}
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
	cmd.Flags().StringVar(&pattern, "pattern", "", "Exact group name")
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
			group := args[0]
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.group.create.preflight",
				Target:   audit.EventTarget{ResourceType: "group", Resource: group},
				Metadata: mutationValueMetadata("mq.group.create.preflight", map[string]string{"group": group}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (mqgov.GroupCoordinate, error) {
				return groupCoord(f, meta, backend, group)
			}, func(mqgov.GroupCoordinate) int { return 1 })
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationCreateGroup, mqclass.Target{Group: group}, ""); err != nil {
				return err
			}
			coordinate := preflight.Value
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.group.create",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "group", Resource: group},
				Metadata: mutationValueMetadata("mq.group.create", coordinate),
			})
			if err != nil {
				return err
			}
			desc, operationErr := preflight.Backend.CreateGroup(cmd.Context(), coordinate)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "GroupDescription", desc, preflight.Target, operationTargetWrite)
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
			group := args[0]
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.group.delete.preflight",
				Target:   audit.EventTarget{ResourceType: "group", Resource: group},
				Metadata: mutationValueMetadata("mq.group.delete.preflight", map[string]string{"group": group}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (mqgov.GroupCoordinate, error) {
				return groupCoord(f, meta, backend, group)
			}, func(mqgov.GroupCoordinate) int { return 1 })
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationDeleteGroup, mqclass.Target{Group: group}, ""); err != nil {
				return err
			}
			coordinate := preflight.Value
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.group.delete",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "group", Resource: group},
				Metadata: mutationValueMetadata("mq.group.delete", coordinate),
			})
			if err != nil {
				return err
			}
			operationErr := preflight.Backend.DeleteGroup(cmd.Context(), coordinate)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "DeleteResult", map[string]string{"group": group, "status": audit.StatusSuccess}, preflight.Target, operationTargetWrite)
		},
	}
}

func newGroupLagCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "lag GROUP TOPIC",
		Short: "Show group lag",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			group, topic := args[0], args[1]
			plan, opTarget, err := runMandatoryBrokerRead(f, readAuditSpec{
				Action:   "mq.group.lag",
				Target:   audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic},
				Metadata: mutationValueMetadata("mq.group.lag", map[string]string{"group": group, "topic": topic}),
			}, func(meta mqgovctx.Context) error {
				target := declaredTopicTarget(meta, firstNonEmpty(f.Backend, meta.Backend, defaultFakeBackend), topic, false)
				target.Group = group
				return classifyAndAuthorize(f, meta, mqclass.OperationLag, target, "")
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (mqgov.OffsetPlan, error) {
				manager, ok := mqgov.SupportsOffsets(backend)
				if !ok {
					return mqgov.OffsetPlan{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support offsets", nil)
				}
				resolved, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
				if resolveErr != nil {
					return mqgov.OffsetPlan{}, resolveErr
				}
				target := resolved.Classification
				target.Group = group
				if authorizeErr := classifyAndAuthorize(f, meta, mqclass.OperationLag, target, ""); authorizeErr != nil {
					return mqgov.OffsetPlan{}, authorizeErr
				}
				groupCoordinate, coordinateErr := groupCoord(f, meta, backend, group)
				if coordinateErr != nil {
					return mqgov.OffsetPlan{}, coordinateErr
				}
				return manager.Lag(cmd.Context(), groupCoordinate, resolved.Coordinate)
			}, func(plan mqgov.OffsetPlan) int {
				return len(plan.Impact)
			})
			if err != nil {
				return err
			}
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
			group, topic := args[0], args[1]
			dryRun := f.DryRun || f.Plan
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.offset.reset.preflight",
				Target:   audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic},
				Metadata: mutationValueMetadata("mq.offset.reset.preflight", map[string]any{"group": group, "topic": topic, "to": to, "dryRun": dryRun}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (offsetResetPreflight, error) {
				manager, ok := mqgov.SupportsOffsets(backend)
				if !ok {
					return offsetResetPreflight{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support offsets", nil)
				}
				resolved, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, dryRun)
				if resolveErr != nil {
					return offsetResetPreflight{}, resolveErr
				}
				target := resolved.Classification
				target.Group = group
				readOperation := mqclass.OperationLag
				if dryRun {
					readOperation = mqclass.OperationResetOffset
				}
				if authorizeErr := classifyAndAuthorize(f, meta, readOperation, target, ""); authorizeErr != nil {
					return offsetResetPreflight{}, authorizeErr
				}
				groupCoordinate, coordinateErr := groupCoord(f, meta, backend, group)
				if coordinateErr != nil {
					return offsetResetPreflight{}, coordinateErr
				}
				request := mqgov.OffsetPlanRequest{Group: groupCoordinate, Topic: resolved.Coordinate, To: to, DryRun: dryRun, Partition: -1}
				previewRequest := request
				previewRequest.DryRun = true
				preview, previewErr := manager.PlanOffsetReset(cmd.Context(), previewRequest)
				return offsetResetPreflight{Manager: manager, Resolved: resolved, Request: request, Preview: preview}, previewErr
			}, func(value offsetResetPreflight) int {
				return len(value.Preview.Impact)
			})
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			allow := allowOffsetReset
			if dryRun {
				allow = ""
			}
			target := preflight.Value.Resolved.Classification
			target.Group = group
			if !dryRun {
				if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationResetOffset, target, allow); err != nil {
					return err
				}
			}
			req := preflight.Value.Request
			if req.DryRun {
				return targetJSONData(f, "OffsetPlan", preflight.Value.Preview, preflight.Target, operationTargetWrite)
			}
			preview := preflight.Value.Preview
			handle, auditErr := beginMutationAudit(f, mutationAuditSpec{
				Action:  "mq.offset.reset",
				Context: preflight.Context,
				Target:  audit.EventTarget{ResourceType: "offset", Resource: group + "/" + topic},
				Metadata: mutationValueMetadata("mq.offset.reset", struct {
					Request mqgov.OffsetPlanRequest
					Preview mqgov.OffsetPlan
				}{Request: req, Preview: preview}),
			})
			if auditErr != nil {
				return auditErr
			}
			plan, operationErr := preflight.Value.Manager.ResetOffset(cmd.Context(), req)
			outcome := plan.BatchOutcome
			total := outcome.Count()
			if outcome.Empty() {
				outcome.Succeeded = len(plan.Impact)
				if operationErr != nil {
					outcome.Succeeded = 0
					outcome.Failed = 1
				}
				total = len(plan.Impact)
				if total == 0 && operationErr != nil {
					total = 1
				}
			}
			if auditErr := finishBatchMutationAuditWithOutcome(handle, total, outcome, operationErr); auditErr != nil {
				return auditErr
			}
			return targetJSONData(f, "OffsetPlan", plan, preflight.Target, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&to, "to", "earliest", "Target offset: earliest | latest | offset:N | datetime:<RFC3339> | shift:±N (some targets are backend-specific and unsupported backends return clear errors)")
	return cmd
}

type offsetResetPreflight struct {
	Manager  mqgov.OffsetManager
	Resolved resolvedTopicTarget
	Request  mqgov.OffsetPlanRequest
	Preview  mqgov.OffsetPlan
}

func groupCoord(f *cliFlags, meta mqgovctx.Context, backend mqgov.Broker, group string) (mqgov.GroupCoordinate, error) {
	scope, err := canonicalBrokerScope(f, meta, backend)
	if err != nil {
		return mqgov.GroupCoordinate{}, err
	}
	return mqgov.GroupCoordinate{Cluster: scope.Cluster, Namespace: scope.Namespace, Group: group}, nil
}
