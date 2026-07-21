package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type aclFlags struct {
	principal    string
	host         string
	vhost        string
	resourceType string
	resourceName string
	patternType  string
	operation    string
	permission   string
}

func newACLCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "acl", Short: "Inspect and manage broker ACLs"}
	cmd.AddCommand(newACLListCmd(f), newACLGrantCmd(f), newACLRevokeCmd(f))
	return cmd
}

func newACLListCmd(f *cliFlags) *cobra.Command {
	var flags aclFlags
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List broker ACLs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsACL(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support ACL management", nil)
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationListACL, mqclass.Target{ACL: aclClassTarget(flags)}, ""); err != nil {
				return err
			}
			filter := mqgov.ACLFilter{Principal: flags.principal, Vhost: flags.vhost, ResourceType: flags.resourceType, ResourceName: flags.resourceName}
			items, err := manager.ListACLs(cmd.Context(), filter)
			if err != nil {
				appendAuditWarn(f, auditEventACL, meta, audit.EventTarget{ResourceType: "acl"}, audit.StatusFailed, aclFilterDiff(filter), err)
				return err
			}
			appendAuditWarn(f, auditEventACL, meta, audit.EventTarget{ResourceType: "acl"}, audit.StatusSuccess, fmt.Sprintf("list count=%d filter=%s", len(items), aclFilterDiff(filter)), nil)
			if f.Output == "json" {
				return targetJSONList(f, "ACLList", items, len(items), len(items), opTarget)
			}
			rows := make([][]string, 0, len(items))
			for _, item := range items {
				rows = append(rows, []string{item.Principal, item.Host, item.Vhost, item.ResourceType, item.ResourceName, item.PatternType, item.Operation, item.Permission})
			}
			return targetTable(f, []string{"PRINCIPAL", "HOST", "VHOST", "RESOURCE TYPE", "RESOURCE NAME", "PATTERN", "OPERATION", "PERMISSION"}, rows, opTarget)
		},
	}
	cmd.Flags().StringVar(&flags.principal, "principal", "", "ACL principal filter")
	cmd.Flags().StringVar(&flags.vhost, "vhost", "/", "RabbitMQ vhost filter")
	cmd.Flags().StringVar(&flags.resourceType, "resource-type", "", "ACL resource type filter")
	cmd.Flags().StringVar(&flags.resourceName, "resource-name", "", "ACL resource name filter")
	return cmd
}

func newACLGrantCmd(f *cliFlags) *cobra.Command {
	var flags aclFlags
	cmd := &cobra.Command{
		Use:   "grant",
		Short: "Grant a broker ACL",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if contextPlanOnly(f) {
				binding := aclBinding(flags)
				return printBrokerChangePlan(f, "grant", "acl", binding.ResourceType+"/"+binding.ResourceName, map[string]any{
					"principal":  binding.Principal,
					"operation":  binding.Operation,
					"permission": binding.Permission,
				})
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsACL(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support ACL management", nil)
			}
			binding := aclBinding(flags)
			target := aclClassTarget(flags)
			allow := safety.AllowFlag("")
			if mqclass.Classify(mqclass.OperationGrantACL, mqclass.Target{ACL: target}).Risk == safety.R3 {
				allow = allowDestructiveACL
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationGrantACL, mqclass.Target{ACL: target}, allow); err != nil {
				return err
			}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.acl.grant",
				Context:  meta,
				Target:   aclAuditTarget(binding),
				Metadata: mutationValueMetadata("mq.acl.grant", binding),
			})
			if err != nil {
				return err
			}
			operationErr := manager.GrantACL(cmd.Context(), binding)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "ACLBinding", binding, opTarget, operationTargetWrite)
		},
	}
	addACLBindingFlags(cmd, &flags)
	return cmd
}

func newACLRevokeCmd(f *cliFlags) *cobra.Command {
	var flags aclFlags
	cmd := &cobra.Command{
		Use:   "revoke",
		Short: "Revoke a broker ACL",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if contextPlanOnly(f) {
				binding := aclBinding(flags)
				return printBrokerChangePlan(f, "revoke", "acl", binding.ResourceType+"/"+binding.ResourceName, map[string]any{
					"principal":  binding.Principal,
					"operation":  binding.Operation,
					"permission": binding.Permission,
				})
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, ok := mqgov.SupportsACL(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support ACL management", nil)
			}
			binding := aclBinding(flags)
			if err := classifyAndAuthorize(f, meta, mqclass.OperationRevokeACL, mqclass.Target{ACL: aclClassTarget(flags)}, allowDestructiveACL); err != nil {
				return err
			}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.acl.revoke",
				Context:  meta,
				Target:   aclAuditTarget(binding),
				Metadata: mutationValueMetadata("mq.acl.revoke", binding),
			})
			if err != nil {
				return err
			}
			operationErr := manager.RevokeACL(cmd.Context(), binding)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "ACLBinding", binding, opTarget, operationTargetWrite)
		},
	}
	addACLBindingFlags(cmd, &flags)
	return cmd
}

func addACLBindingFlags(cmd *cobra.Command, flags *aclFlags) {
	cmd.Flags().StringVar(&flags.principal, "principal", "", "ACL principal")
	cmd.Flags().StringVar(&flags.host, "host", "*", "ACL host")
	cmd.Flags().StringVar(&flags.vhost, "vhost", "/", "RabbitMQ vhost")
	cmd.Flags().StringVar(&flags.resourceType, "resource-type", "", "ACL resource type")
	cmd.Flags().StringVar(&flags.resourceName, "resource-name", "", "ACL resource name")
	cmd.Flags().StringVar(&flags.patternType, "pattern", "literal", "ACL resource pattern: literal | prefixed | regex")
	cmd.Flags().StringVar(&flags.operation, "operation", "", "ACL operation")
	cmd.Flags().StringVar(&flags.permission, "permission", "", "ACL permission: allow | deny")
}

func aclBinding(flags aclFlags) mqgov.ACLBinding {
	return mqgov.ACLBinding{
		Principal:    flags.principal,
		Host:         flags.host,
		Vhost:        flags.vhost,
		ResourceType: flags.resourceType,
		ResourceName: flags.resourceName,
		PatternType:  flags.patternType,
		Operation:    flags.operation,
		Permission:   flags.permission,
	}
}

func aclClassTarget(flags aclFlags) mqclass.ACLTarget {
	return mqclass.ACLTarget{
		Principal:    flags.principal,
		ResourceType: flags.resourceType,
		ResourceName: flags.resourceName,
		PatternType:  flags.patternType,
		Operation:    flags.operation,
		Permission:   flags.permission,
	}
}

func aclAuditTarget(binding mqgov.ACLBinding) audit.EventTarget {
	return audit.EventTarget{ResourceType: "acl", Resource: binding.ResourceType + "/" + binding.ResourceName}
}

func aclFilterDiff(filter mqgov.ACLFilter) string {
	return "principal=" + filter.Principal +
		" vhost=" + filter.Vhost +
		" resourceType=" + filter.ResourceType +
		" resourceName=" + filter.ResourceName
}
