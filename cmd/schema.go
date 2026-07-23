package cmd

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
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
			if err := validateExactPattern(pattern); err != nil {
				return err
			}
			options := mqgov.SchemaListOptions{Pattern: pattern, Limit: limit}
			items, opTarget, err := runMandatoryBrokerRead(f, readAuditSpec{
				Action:   "mq.schema.list",
				Target:   audit.EventTarget{ResourceType: "schema"},
				Metadata: mutationValueMetadata("mq.schema.list", options),
			}, func(meta mqgovctx.Context) error {
				return classifyAndAuthorize(f, meta, mqclass.OperationListSchema, mqclass.Target{Topic: pattern}, "")
			}, func(backend mqgov.Broker, _ mqgovctx.Context) ([]mqgov.SchemaSubject, error) {
				manager, managerErr := schemaManager(backend)
				if managerErr != nil {
					return nil, managerErr
				}
				return manager.ListSchemas(cmd.Context(), options)
			}, func(items []mqgov.SchemaSubject) int {
				return len(items)
			})
			if err != nil {
				return err
			}
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
			subject := args[0]
			request := mqgov.SchemaDescribeRequest{Subject: subject, Version: version}
			result, opTarget, err := runMandatorySchemaRead(f, readAuditSpec{
				Action:   "mq.schema.describe",
				Target:   audit.EventTarget{ResourceType: "schema", Resource: subject},
				Metadata: mutationValueMetadata("mq.schema.describe", request),
			}, func(meta mqgovctx.Context) error {
				return classifyAndAuthorize(f, meta, mqclass.OperationDescribeSchema, mqclass.Target{Topic: subject}, "")
			}, func(manager mqgov.SchemaManager) (mqgov.SchemaDescription, error) {
				return manager.DescribeSchema(cmd.Context(), request)
			})
			if err != nil {
				return err
			}
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
			subject := args[0]
			schema, err := readSchemaInput(schemaText, schemaFile)
			if err != nil {
				return err
			}
			request := mqgov.SchemaCheckRequest{Subject: subject, Version: version, Type: schemaType, Schema: schema}
			result, opTarget, err := runMandatorySchemaRead(f, readAuditSpec{
				Action:   "mq.schema.check",
				Target:   audit.EventTarget{ResourceType: "schema", Resource: subject},
				Metadata: mutationValueMetadata("mq.schema.check", request),
			}, func(meta mqgovctx.Context) error {
				return classifyAndAuthorize(f, meta, mqclass.OperationCheckSchema, mqclass.Target{Topic: subject}, "")
			}, func(manager mqgov.SchemaManager) (mqgov.SchemaCheckResult, error) {
				return manager.CheckCompatibility(cmd.Context(), request)
			})
			if err != nil {
				return err
			}
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
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.schema.register.preflight",
				Target:   audit.EventTarget{ResourceType: "schema", Resource: subject},
				Metadata: mutationPayloadMetadata("mq.schema.register.preflight", []byte(schema)),
			}, func(backend mqgov.Broker, _ mqgovctx.Context) (schemaRegisterPreflight, error) {
				manager, managerErr := schemaManager(backend)
				if managerErr != nil {
					return schemaRegisterPreflight{}, managerErr
				}
				value := schemaRegisterPreflight{Manager: manager, Target: mqclass.Target{Topic: subject}}
				existing, describeErr := manager.DescribeSchema(cmd.Context(), mqgov.SchemaDescribeRequest{Subject: subject, Version: "latest"})
				if describeErr != nil {
					if isResourceNotFound(describeErr) {
						return value, nil
					}
					value.Target.SchemaUnknown = true
					return value, describeErr
				}
				value.Target.SchemaExists = true
				check, checkErr := manager.CheckCompatibility(cmd.Context(), mqgov.SchemaCheckRequest{Subject: subject, Version: existing.Version, Type: schemaType, Schema: schema})
				if checkErr != nil {
					return value, checkErr
				}
				if !check.Compatible {
					return value, apperrors.New(apperrors.CodeValidationFailed, "schema is not compatible with existing subject", nil)
				}
				return value, nil
			}, func(schemaRegisterPreflight) int { return 1 })
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationRegisterSchema, preflight.Value.Target, ""); err != nil {
				return err
			}
			request := mqgov.SchemaRegisterRequest{Subject: subject, Type: schemaType, Schema: schema}
			metadata := mutationPayloadMetadata("mq.schema.register", []byte(schema))
			metadata.Items = 1
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.schema.register",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "schema", Resource: subject},
				Metadata: metadata,
			})
			if err != nil {
				return err
			}
			result, operationErr := preflight.Value.Manager.RegisterSchema(cmd.Context(), request)
			if err := finishMutationAudit(handle, mutationAuditOutcome{Revision: result.Version}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "SchemaDescription", result, preflight.Target, operationTargetWrite)
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
			subject := args[0]
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.schema.delete.preflight",
				Target:   audit.EventTarget{ResourceType: "schema", Resource: subject},
				Metadata: mutationValueMetadata("mq.schema.delete.preflight", map[string]any{"subject": subject, "version": version, "permanent": permanent}),
			}, func(backend mqgov.Broker, _ mqgovctx.Context) (schemaDeletePreflight, error) {
				manager, managerErr := schemaManager(backend)
				if managerErr != nil {
					return schemaDeletePreflight{}, managerErr
				}
				desc, describeErr := manager.DescribeSchema(cmd.Context(), mqgov.SchemaDescribeRequest{Subject: subject, Version: firstNonEmpty(version, "latest")})
				return schemaDeletePreflight{Manager: manager, Description: desc}, describeErr
			}, func(schemaDeletePreflight) int { return 1 })
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationDeleteSchema, mqclass.Target{Topic: subject}, allowSchemaDelete); err != nil {
				return err
			}
			request := mqgov.SchemaDeleteRequest{Subject: subject, Version: version, Permanent: permanent}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.schema.delete",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "schema", Resource: subject},
				Metadata: mutationValueMetadata("mq.schema.delete", request),
			})
			if err != nil {
				return err
			}
			result, operationErr := preflight.Value.Manager.DeleteSchema(cmd.Context(), request)
			revision := firstNonEmpty(result.Version, preflight.Value.Description.Version)
			if err := finishMutationAudit(handle, mutationAuditOutcome{Revision: revision}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "SchemaDeleteResult", result, preflight.Target, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "Schema version to delete; omit to delete the subject")
	cmd.Flags().BoolVar(&permanent, "permanent", false, "Permanently delete the schema subject or version when supported")
	return cmd
}

type schemaRegisterPreflight struct {
	Manager mqgov.SchemaManager
	Target  mqclass.Target
}

type schemaDeletePreflight struct {
	Manager     mqgov.SchemaManager
	Description mqgov.SchemaDescription
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

func isResourceNotFound(err error) bool {
	return apperrors.AsAppError(err).Code == apperrors.CodeResourceNotFound
}
