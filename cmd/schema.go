package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func newSchemaCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "schema", Short: "Inspect and govern broker-native schemas"}
	cmd.AddCommand(newSchemaListCmd(f), newSchemaDescribeCmd(f), newSchemaCheckCmd(f), newSchemaRegisterCmd(f), newSchemaDeleteCmd(f))
	return cmd
}

func newSchemaListCmd(f *cliFlags) *cobra.Command {
	var pattern string
	var limit int
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List schema subjects",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, err := schemaManager(backend)
			if err != nil {
				return err
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationListSchema, mqclass.Target{Topic: pattern}, ""); err != nil {
				return err
			}
			items, err := manager.ListSchemas(cmd.Context(), mqgov.SchemaListOptions{Pattern: pattern, Limit: limit})
			if err != nil {
				appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema"}, audit.StatusFailed, "list", err)
				return err
			}
			appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema"}, audit.StatusSuccess, fmt.Sprintf("list count=%d", len(items)), nil)
			if f.Output == "json" {
				return targetJSONList(f, "SchemaList", items, len(items), len(items), opTarget)
			}
			rows := make([][]string, 0, len(items))
			for _, item := range items {
				rows = append(rows, []string{item.Subject})
			}
			return targetTable(f, []string{"SUBJECT"}, rows, opTarget)
		},
	}
	cmd.Flags().StringVar(&pattern, "pattern", "", "Exact schema subject")
	cmd.Flags().IntVar(&limit, "limit", 0, "Maximum subjects")
	return cmd
}

func newSchemaDescribeCmd(f *cliFlags) *cobra.Command {
	var version string
	cmd := &cobra.Command{
		Use:   "describe SUBJECT",
		Short: "Describe a schema subject version",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, err := schemaManager(backend)
			if err != nil {
				return err
			}
			subject := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationDescribeSchema, mqclass.Target{Topic: subject}, ""); err != nil {
				return err
			}
			result, err := manager.DescribeSchema(cmd.Context(), mqgov.SchemaDescribeRequest{Subject: subject, Version: version})
			if err != nil {
				appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema", Resource: subject}, audit.StatusFailed, "describe", err)
				return err
			}
			appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema", Resource: subject}, audit.StatusSuccess, schemaMetaDiff(result), nil)
			return targetJSONData(f, "SchemaDescription", result, opTarget, operationTargetRead)
		},
	}
	cmd.Flags().StringVar(&version, "version", "latest", "Schema version or latest")
	return cmd
}

func newSchemaCheckCmd(f *cliFlags) *cobra.Command {
	var version string
	var schemaType string
	var schemaText string
	var schemaFile string
	cmd := &cobra.Command{
		Use:   "check SUBJECT",
		Short: "Check schema compatibility without registering it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, err := schemaManager(backend)
			if err != nil {
				return err
			}
			subject := args[0]
			schema, err := readSchemaInput(schemaText, schemaFile)
			if err != nil {
				return err
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationCheckSchema, mqclass.Target{Topic: subject}, ""); err != nil {
				return err
			}
			result, err := manager.CheckCompatibility(cmd.Context(), mqgov.SchemaCheckRequest{Subject: subject, Version: version, Type: schemaType, Schema: schema})
			if err != nil {
				appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema", Resource: subject}, audit.StatusFailed, "check schemaSha256="+mqgov.SHA256Hex([]byte(schema)), err)
				return err
			}
			appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema", Resource: subject}, audit.StatusSuccess, fmt.Sprintf("check compatible=%t schemaSha256=%s", result.Compatible, result.SchemaHash), nil)
			return targetJSONData(f, "SchemaCheckResult", result, opTarget, operationTargetRead)
		},
	}
	cmd.Flags().StringVar(&version, "version", "latest", "Schema version or latest")
	cmd.Flags().StringVar(&schemaType, "schema-type", "", "Schema type, for example AVRO, JSON, PROTOBUF, or STRING")
	cmd.Flags().StringVar(&schemaText, "schema", "", "Schema text to check")
	cmd.Flags().StringVar(&schemaFile, "schema-file", "", "File containing schema text to check")
	return cmd
}

func newSchemaRegisterCmd(f *cliFlags) *cobra.Command {
	var schemaType string
	var schemaText string
	var schemaFile string
	cmd := &cobra.Command{
		Use:   "register SUBJECT",
		Short: "Register a schema subject or a new subject version",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			subject := args[0]
			schema, err := readSchemaInput(schemaText, schemaFile)
			if err != nil {
				return err
			}
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "register", "schema", subject, map[string]any{
					"schemaType":   schemaType,
					"schemaSha256": mqgov.SHA256Hex([]byte(schema)),
				})
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, err := schemaManager(backend)
			if err != nil {
				return err
			}
			target := mqclass.Target{Topic: subject}
			existing, describeErr := manager.DescribeSchema(cmd.Context(), mqgov.SchemaDescribeRequest{Subject: subject, Version: "latest"})
			if describeErr == nil {
				target.SchemaExists = true
			} else if !isResourceNotFound(describeErr) {
				target.SchemaUnknown = true
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationRegisterSchema, target, ""); err != nil {
				return err
			}
			if describeErr != nil && target.SchemaUnknown {
				appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema", Resource: subject}, audit.StatusFailed, "register schemaSha256="+mqgov.SHA256Hex([]byte(schema)), describeErr)
				return describeErr
			}
			if target.SchemaExists {
				check, err := manager.CheckCompatibility(cmd.Context(), mqgov.SchemaCheckRequest{Subject: subject, Version: existing.Version, Type: schemaType, Schema: schema})
				if err != nil {
					appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema", Resource: subject}, audit.StatusFailed, "register schemaSha256="+mqgov.SHA256Hex([]byte(schema)), err)
					return err
				}
				if !check.Compatible {
					err := apperrors.New(apperrors.CodeValidationFailed, "schema is not compatible with existing subject", nil)
					appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema", Resource: subject}, audit.StatusFailed, fmt.Sprintf("register compatible=false schemaSha256=%s", check.SchemaHash), err)
					return err
				}
			}
			request := mqgov.SchemaRegisterRequest{Subject: subject, Type: schemaType, Schema: schema}
			metadata := mutationPayloadMetadata("mq.schema.register", []byte(schema))
			metadata.Items = 1
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.schema.register",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "schema", Resource: subject},
				Metadata: metadata,
			})
			if err != nil {
				return err
			}
			result, operationErr := manager.RegisterSchema(cmd.Context(), request)
			if err := finishMutationAudit(handle, mutationAuditOutcome{Revision: result.Version}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "SchemaDescription", result, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&schemaType, "schema-type", "", "Schema type, for example AVRO, JSON, PROTOBUF, or STRING")
	cmd.Flags().StringVar(&schemaText, "schema", "", "Schema text to register")
	cmd.Flags().StringVar(&schemaFile, "schema-file", "", "File containing schema text to register")
	return cmd
}

func newSchemaDeleteCmd(f *cliFlags) *cobra.Command {
	var version string
	var permanent bool
	cmd := &cobra.Command{
		Use:   "delete SUBJECT",
		Short: "Delete a schema subject or version",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "delete", "schema", args[0], map[string]any{
					"version":   version,
					"permanent": permanent,
				})
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			manager, err := schemaManager(backend)
			if err != nil {
				return err
			}
			subject := args[0]
			desc, err := manager.DescribeSchema(cmd.Context(), mqgov.SchemaDescribeRequest{Subject: subject, Version: firstNonEmpty(version, "latest")})
			if err != nil {
				appendAuditWarn(f, auditEventSchema, meta, audit.EventTarget{ResourceType: "schema", Resource: subject}, audit.StatusFailed, "delete", err)
				return err
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationDeleteSchema, mqclass.Target{Topic: subject}, allowSchemaDelete); err != nil {
				return err
			}
			request := mqgov.SchemaDeleteRequest{Subject: subject, Version: version, Permanent: permanent}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.schema.delete",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "schema", Resource: subject},
				Metadata: mutationValueMetadata("mq.schema.delete", request),
			})
			if err != nil {
				return err
			}
			result, operationErr := manager.DeleteSchema(cmd.Context(), request)
			revision := firstNonEmpty(result.Version, desc.Version)
			if err := finishMutationAudit(handle, mutationAuditOutcome{Revision: revision}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "SchemaDeleteResult", result, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "Schema version to delete; omit to delete the subject")
	cmd.Flags().BoolVar(&permanent, "permanent", false, "Permanently delete the schema subject or version when supported")
	return cmd
}

func schemaManager(backend mqgov.Broker) (mqgov.SchemaManager, error) {
	if !backend.Capabilities().SupportsSchema {
		return nil, apperrors.New(apperrors.CodeNotImplemented, "backend does not support schema registry", nil)
	}
	manager, ok := mqgov.SupportsSchema(backend)
	if !ok {
		return nil, apperrors.New(apperrors.CodeNotImplemented, "backend does not support schema registry", nil)
	}
	return manager, nil
}

func readSchemaInput(schemaText, schemaFile string) (string, error) {
	if schemaText != "" && schemaFile != "" {
		return "", apperrors.New(apperrors.CodeUsageError, "--schema and --schema-file are mutually exclusive", nil)
	}
	if schemaText != "" {
		return schemaText, nil
	}
	if schemaFile == "" {
		return "", apperrors.New(apperrors.CodeUsageError, "schema text is required", nil)
	}
	data, err := os.ReadFile(schemaFile) //nolint:gosec // Operator-supplied schema file path for local compatibility checks.
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to read schema file", err)
	}
	return string(data), nil
}

func schemaMetaDiff(desc mqgov.SchemaDescription) string {
	return "describe subject=" + desc.Subject + " version=" + desc.Version + " id=" + strconv.Itoa(desc.ID) + " schemaSha256=" + desc.SchemaHash
}

func isResourceNotFound(err error) bool {
	return apperrors.AsAppError(err).Code == apperrors.CodeResourceNotFound
}
