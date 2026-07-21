package cmd

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/credstore"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"
	"github.com/JiangHe12/opskit-core/v2/redact"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

const (
	ctxExportAPIVersion          = "mqgov-cli.io/ctx-export/v1"
	redactedCredential           = "<REDACTED>"
	credentialBackendEncrypted   = "encrypted-file"
	credentialBackendKeychain    = "keychain"
	credentialMigrationEventType = audit.EventType("credential.migrate")
	credentialCompensationLimit  = 30 * time.Second
)

var contextSetUpdate = mqgovctx.Update

type ctxSetOptions struct {
	credentialBackend           string
	protected                   bool
	password                    string
	cluster                     string
	namespace                   string
	kafkaBrokers                string
	kafkaSASL                   string
	kafkaSchemaRegistryURL      string
	kafkaSchemaRegistryUsername string
	kafkaSchemaRegistryPassword string
	rabbitAMQPURL               string
	rabbitManagement            string
	rabbitUsername              string
	rabbitHost                  string
	rabbitPort                  int
	rabbitVHost                 string
	pulsarServiceURL            string
	pulsarAdminURL              string
	pulsarTenant                string
	pulsarNamespace             string
	rocketNameServers           string
	rocketBrokerAddr            string
	rocketAccessKey             string
	tls                         bool
	caCert                      string
	clientCert                  string
	clientKey                   string
}

type contextExportDocument struct {
	APIVersion string                      `yaml:"apiVersion"`
	Name       string                      `yaml:"name"`
	Context    *mqgovctx.Context           `yaml:"context,omitempty"`
	Contexts   map[string]mqgovctx.Context `yaml:"contexts,omitempty"`
}

type contextImportResult struct {
	Name               string   `json:"name"`
	Names              []string `json:"names,omitempty"`
	Count              int      `json:"count"`
	CredentialRedacted bool     `json:"credentialRedacted"`
}

type ctxExportOptions struct {
	includeCredentials bool
	outputFile         string
	all                bool
}

type ctxImportOptions struct {
	file   string
	rename string
	force  bool
}

type roleOptions struct {
	targetOperator string
	role           string
}

type roleItem struct {
	Operator string `json:"operator"`
	Role     string `json:"role"`
}

type migrateCredentialsOptions struct {
	toBackend   string
	contextName string
	dryRun      bool
}

type migrateCredentialCandidate struct {
	name                   string
	context                mqgovctx.Context
	password               string
	schemaRegistryPassword string
}

type credentialMigrationResult struct {
	DryRun      bool     `json:"dryRun"`
	Backend     string   `json:"backend"`
	Contexts    []string `json:"contexts"`
	Credentials int      `json:"credentials"`
}

type credentialMigrationProgress struct {
	succeeded int
	failed    int
	uncertain int
}

type credentialMigrationWrite struct {
	key          string
	slot         credentialPhysicalSlot
	owner        string
	previous     string
	written      string
	existed      bool
	putSucceeded bool
}

type credentialMigrationTransaction struct {
	backend credstore.Backend
	writes  []credentialMigrationWrite
}

func contextPlanOnly(f *cliFlags) bool {
	return f.DryRun || f.Plan
}

type contextControlPolicy struct {
	meta         mqgovctx.Context
	source       string
	targetExists bool
}

func contextPreChangePolicy(cfg *corectx.Config[mqgovctx.Context], target string) (contextControlPolicy, error) {
	if item, ok := cfg.Contexts[target]; ok {
		return contextControlPolicy{meta: item, source: target, targetExists: true}, nil
	}
	if cfg.CurrentContext != "" {
		if item, ok := cfg.Contexts[cfg.CurrentContext]; ok {
			return contextControlPolicy{meta: item, source: cfg.CurrentContext}, nil
		}
		return contextControlPolicy{}, apperrors.New(apperrors.CodeLocalIOError, fmt.Sprintf("current context %q is missing from the context store", cfg.CurrentContext), nil)
	}
	return contextControlPolicy{}, nil
}

func ensureContextPolicyUnchanged(cfg *corectx.Config[mqgovctx.Context], target string, expected contextControlPolicy) error {
	actual, err := contextPreChangePolicy(cfg, target)
	if err != nil {
		return err
	}
	if !sameContextControlPolicy(actual, expected) {
		return apperrors.New(apperrors.CodeAuthorizationRequired, "context policy changed during authorization; retry the command", nil)
	}
	return nil
}

func contextUsePreChangePolicy(cfg *corectx.Config[mqgovctx.Context], target string) (contextControlPolicy, error) {
	targetItem, ok := cfg.Contexts[target]
	if !ok {
		return contextControlPolicy{}, apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", target), nil)
	}
	if cfg.CurrentContext != "" {
		if current, exists := cfg.Contexts[cfg.CurrentContext]; exists {
			return contextControlPolicy{meta: current, source: cfg.CurrentContext, targetExists: true}, nil
		}
		return contextControlPolicy{}, apperrors.New(apperrors.CodeLocalIOError, fmt.Sprintf("current context %q is missing from the context store", cfg.CurrentContext), nil)
	}
	return contextControlPolicy{meta: targetItem, source: target, targetExists: true}, nil
}

func ensureContextUsePolicyUnchanged(cfg *corectx.Config[mqgovctx.Context], target string, expected contextControlPolicy) error {
	actual, err := contextUsePreChangePolicy(cfg, target)
	if err != nil {
		return err
	}
	if !sameContextControlPolicy(actual, expected) {
		return apperrors.New(apperrors.CodeAuthorizationRequired, "current context policy changed during authorization; retry the command", nil)
	}
	return nil
}

func sameContextControlPolicy(actual, expected contextControlPolicy) bool {
	return actual.source == expected.source &&
		actual.targetExists == expected.targetExists &&
		actual.meta.Env == expected.meta.Env &&
		actual.meta.Protected == expected.meta.Protected &&
		actual.meta.TicketPattern == expected.meta.TicketPattern &&
		actual.meta.TicketValidator == expected.meta.TicketValidator &&
		reflect.DeepEqual(actual.meta.Roles, expected.meta.Roles)
}

func authorizeContextControl(f *cliFlags, contextName string, preChange contextControlPolicy, allow safety.AllowFlag) error {
	operator, err := trustedOperatorIdentity(f)
	if err != nil {
		return err
	}
	policyContextName := firstNonEmpty(preChange.source, contextName)
	meta := preChange.meta
	err = safety.Authorize(safety.R3, safety.Options{
		Yes:                f.Yes,
		NonInteractive:     f.NonInter,
		Ticket:             f.Ticket,
		TicketPattern:      meta.TicketPattern,
		Validator:          ticketValidator(meta.TicketValidator, policyContextName, operator),
		RequiredAllowFlags: []safety.AllowFlag{allow},
		GrantedAllowFlags:  grantedAllowFlags(f),
		Roles:              meta.Roles,
		Operator:           operator,
	})
	if err != nil {
		appendAuditWarn(f, audit.EventAuthorizationDenied, meta, audit.EventTarget{ResourceType: "context", Resource: contextName}, audit.StatusDenied, "", err)
	}
	return err
}

func printContextControlPlan(f *cliFlags, action, contextName string, item mqgovctx.Context) error {
	return newPrinter(f).JSONData("ChangePlan", map[string]any{
		"resourceType": "context",
		"action":       action,
		"context":      contextName,
		"protected":    item.Protected,
		"dryRun":       true,
	})
}

func newContextCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "ctx", Aliases: []string{"context"}, Short: "Manage mqgov contexts", Args: requireSubcommand, RunE: runParentHelp}
	cmd.AddCommand(ctxSetCmd(f), ctxUseCmd(f), ctxListCmd(f), ctxCurrentCmd(f), ctxDeleteCmd(f), ctxExportCmd(f), ctxImportCmd(f), ctxRoleCmd(f), ctxMigrateCredentialsCmd(f), ctxTestCmd(f))
	return cmd
}

func ctxSetCmd(f *cliFlags) *cobra.Command { //nolint:gocyclo // Backend-specific context flags stay local to ctx set.
	var opts ctxSetOptions
	cmd := &cobra.Command{
		Use:   "set <name>",
		Short: "Set a backend-bound context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if f.Backend == "" {
				return apperrors.New(apperrors.CodeUsageError, "--backend is required", nil)
			}
			if !supportedContextBackend(f.Backend) {
				return apperrors.New(apperrors.CodeNotImplemented, "backend is not supported", nil)
			}
			if err := credstore.RequireSecureBackend(opts.credentialBackend, opts.password != "" || opts.kafkaSchemaRegistryPassword != ""); err != nil {
				return err
			}
			item := mqgovctx.Context{
				Base: corectx.Base{
					Username:          opts.rabbitUsername,
					Protected:         opts.protected,
					CredentialBackend: opts.credentialBackend,
					OTLPRedact:        true,
				},
				Backend:   f.Backend,
				Cluster:   firstNonEmpty(opts.cluster, f.Cluster),
				Namespace: firstNonEmpty(opts.namespace, f.Namespace),
			}
			applyBackendContextOptions(&item, opts)
			if err := validateContextDefinition(&item); err != nil {
				return err
			}
			cfg, err := mqgovctx.Load()
			if err != nil {
				return err
			}
			preChange, err := contextPreChangePolicy(cfg, args[0])
			if err != nil {
				return err
			}
			if contextPlanOnly(f) {
				appendContextAuditWarn(f, audit.EventType("ctx.set"), preChange.meta, audit.StatusSuccess, "ctx set dryRun=true", nil)
				return printContextControlPlan(f, "set", args[0], item)
			}
			if err := authorizeContextControl(f, args[0], preChange, allowContextChange); err != nil {
				return err
			}
			candidate, err := planContextSetCredentials(
				args[0],
				&item,
				opts.password,
				opts.kafkaSchemaRegistryPassword,
			)
			if err != nil {
				return err
			}
			candidates := []contextImportCredentialCandidate{candidate}
			if err := validateContextImportCredentialCandidates(candidates); err != nil {
				return err
			}
			metadata := mutationValueMetadata("mq.ctx.set", item)
			metadata.Items = 1
			expected := map[string]mqgovctx.Context{args[0]: item}
			var handle *mutationAuditHandle
			var transaction *contextImportCredentialTransaction
			var original map[string]contextImportTargetState
			compensationStatus := ""
			compensationUncertain := false
			compensated := false
			operationErr := contextSetUpdate(func(locked *corectx.Config[mqgovctx.Context]) error {
				if err := ensureContextPolicyUnchanged(locked, args[0], preChange); err != nil {
					return err
				}
				if err := validateContextImportCredentialKeySet(locked, expected); err != nil {
					return err
				}
				original = captureContextImportTargets(locked, []string{args[0]})
				handle, err = beginMutationAudit(f, mutationAuditSpec{
					Action:      "mq.ctx.set",
					ContextName: args[0],
					Context:     preChange.meta,
					Target:      audit.EventTarget{ResourceType: "context", Resource: args[0]},
					Metadata:    metadata,
				})
				if err != nil {
					return err
				}
				transaction, err = storeContextImportCredentials(cmd.Context(), candidates)
				if err != nil {
					compensated = true
					var compensationErr error
					compensationStatus, compensationErr = compensateContextImportCredentialsLocked(cmd.Context(), locked, transaction)
					if compensationErr != nil {
						compensationUncertain = true
						return contextSetCompensationError(err, compensationErr)
					}
					return err
				}
				locked.Contexts[args[0]] = item
				return nil
			})
			if operationErr != nil && transaction != nil && !compensated {
				var reconciliationErr error
				compensationStatus, compensationUncertain, reconciliationErr = reconcileContextImportFailure(
					cmd.Context(),
					transaction,
					original,
					expected,
				)
				if reconciliationErr != nil {
					operationErr = contextSetCompensationError(operationErr, reconciliationErr)
				}
			}
			if handle != nil {
				if err := finishContextImportAudit(handle, 1, compensationStatus, compensationUncertain, operationErr); err != nil {
					return err
				}
			} else if operationErr != nil {
				return operationErr
			}
			return newPrinter(f).JSONData("ContextItem", contextView(args[0], item, false, false))
		},
	}
	cmd.Flags().StringVar(&opts.credentialBackend, "credential-backend", "plain-yaml", "Credential backend")
	cmd.Flags().StringVar(&opts.password, "password", "", "Password, token, or RocketMQ secretKey to store in credstore")
	cmd.Flags().BoolVar(&opts.protected, "protected", false, "Mark context as protected")
	cmd.Flags().StringVar(&opts.cluster, "cluster", "", "Broker cluster")
	cmd.Flags().StringVarP(&opts.namespace, "namespace", "n", "", "Broker namespace")
	cmd.Flags().StringVar(&opts.kafkaBrokers, "brokers", "", "Kafka brokers, comma-separated")
	cmd.Flags().StringVar(&opts.kafkaSASL, "sasl-mechanism", "", "Kafka SASL mechanism")
	cmd.Flags().StringVar(&opts.kafkaSchemaRegistryURL, "schema-registry-url", "", "Kafka Schema Registry URL")
	cmd.Flags().StringVar(&opts.kafkaSchemaRegistryUsername, "schema-registry-username", "", "Kafka Schema Registry username")
	cmd.Flags().StringVar(&opts.kafkaSchemaRegistryPassword, "schema-registry-password", "", "Kafka Schema Registry password to store in credstore")
	cmd.Flags().StringVar(&opts.rabbitAMQPURL, "amqp-url", "", "RabbitMQ AMQP URL")
	cmd.Flags().StringVar(&opts.rabbitManagement, "management-url", "", "RabbitMQ management URL")
	cmd.Flags().StringVar(&opts.rabbitUsername, "username", "", "Broker username (Kafka or RabbitMQ)")
	cmd.Flags().StringVar(&opts.rabbitHost, "host", "", "RabbitMQ host")
	cmd.Flags().IntVar(&opts.rabbitPort, "port", 0, "RabbitMQ port")
	cmd.Flags().StringVar(&opts.rabbitVHost, "vhost", "", "RabbitMQ virtual host")
	cmd.Flags().StringVar(&opts.pulsarServiceURL, "service-url", "", "Pulsar service URL")
	cmd.Flags().StringVar(&opts.pulsarAdminURL, "admin-url", "", "Pulsar admin URL")
	cmd.Flags().StringVar(&opts.pulsarTenant, "tenant", "", "Pulsar tenant")
	cmd.Flags().StringVar(&opts.pulsarNamespace, "pulsar-namespace", "", "Pulsar namespace")
	cmd.Flags().StringVar(&opts.rocketNameServers, "nameservers", "", "RocketMQ NameServer addresses, comma-separated")
	cmd.Flags().StringVar(&opts.rocketBrokerAddr, "broker-addr", "", "RocketMQ broker address for topic creation")
	cmd.Flags().StringVar(&opts.rocketAccessKey, "access-key", "", "RocketMQ ACL accessKey")
	cmd.Flags().BoolVar(&opts.tls, "tls", false, "Enable backend TLS")
	cmd.Flags().StringVar(&opts.caCert, "ca-cert", "", "CA certificate file")
	cmd.Flags().StringVar(&opts.clientCert, "client-cert", "", "mTLS client certificate file")
	cmd.Flags().StringVar(&opts.clientKey, "client-key", "", "mTLS client private key file")
	return cmd
}

func planContextSetCredentials(
	name string,
	item *mqgovctx.Context,
	password string,
	schemaRegistryPassword string,
) (contextImportCredentialCandidate, error) {
	candidate, err := newContextImportCredentialCandidate(name, *item)
	if err != nil {
		return contextImportCredentialCandidate{}, err
	}
	if item.CredentialBackend == "" || item.CredentialBackend == "plain-yaml" {
		item.Password = password
		item.KafkaSchemaRegistryPassword = schemaRegistryPassword
		return candidate, nil
	}
	backend, err := contextImportCredentialBackend(*item)
	if err != nil {
		return contextImportCredentialCandidate{}, err
	}
	candidate.backend = backend
	if password != "" {
		candidate.password = password
		item.Password = credstore.EncodeRef(item.CredentialBackend)
	}
	if schemaRegistryPassword != "" {
		candidate.schemaRegistryPassword = schemaRegistryPassword
		item.KafkaSchemaRegistryPassword = credstore.EncodeRef(item.CredentialBackend)
	}
	return candidate, nil
}

func contextSetCompensationError(operationErr, compensationErr error) error {
	return apperrors.New(
		apperrors.CodeCredentialStoreError,
		"context set failed and credential compensation could not be completed safely",
		errors.Join(operationErr, compensationErr),
	)
}

func supportedContextBackend(backend string) bool {
	switch backend {
	case "kafka", "rabbitmq", "pulsar", "rocketmq":
		return true
	default:
		return false
	}
}

func applyBackendContextOptions(item *mqgovctx.Context, opts ctxSetOptions) {
	switch item.Backend {
	case "kafka":
		item.KafkaBrokers = splitCSV(opts.kafkaBrokers)
		item.KafkaSASLMechanism = opts.kafkaSASL
		item.KafkaTLS = opts.tls
		item.KafkaCACertFile = opts.caCert
		item.KafkaClientCertFile = opts.clientCert
		item.KafkaClientKeyFile = opts.clientKey
		item.KafkaSchemaRegistryURL = opts.kafkaSchemaRegistryURL
		item.KafkaSchemaRegistryUsername = opts.kafkaSchemaRegistryUsername
	case "rabbitmq":
		if opts.rabbitUsername != "" {
			item.Username = opts.rabbitUsername
		}
		item.RabbitMQAMQPURL = opts.rabbitAMQPURL
		item.RabbitMQManagementURL = opts.rabbitManagement
		item.RabbitMQHost = opts.rabbitHost
		item.RabbitMQPort = opts.rabbitPort
		item.RabbitMQVHost = opts.rabbitVHost
		item.RabbitMQTLS = opts.tls
		item.RabbitMQCACertFile = opts.caCert
		item.RabbitMQClientCertFile = opts.clientCert
		item.RabbitMQClientKeyFile = opts.clientKey
	case "pulsar":
		item.PulsarServiceURL = opts.pulsarServiceURL
		item.PulsarAdminURL = opts.pulsarAdminURL
		item.PulsarTenant = opts.pulsarTenant
		item.PulsarNamespace = opts.pulsarNamespace
		item.PulsarTLS = opts.tls
		item.PulsarCACertFile = opts.caCert
		item.PulsarClientCertFile = opts.clientCert
		item.PulsarClientKeyFile = opts.clientKey
	case "rocketmq":
		item.RocketMQNameServers = splitCSV(opts.rocketNameServers)
		item.RocketMQBrokerAddr = opts.rocketBrokerAddr
		item.RocketMQAccessKey = opts.rocketAccessKey
		item.RocketMQTLS = opts.tls
		item.RocketMQCACertFile = opts.caCert
		item.RocketMQClientCertFile = opts.clientCert
		item.RocketMQClientKeyFile = opts.clientKey
	}
}

func ctxUseCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Set current context",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cfg, err := mqgovctx.Load()
			if err != nil {
				return err
			}
			item, ok := cfg.Contexts[args[0]]
			if !ok {
				return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", args[0]), nil)
			}
			preChange, err := contextUsePreChangePolicy(cfg, args[0])
			if err != nil {
				return err
			}
			if contextPlanOnly(f) {
				appendContextAuditWarn(f, audit.EventType("ctx.use"), item, audit.StatusSuccess, "ctx use dryRun=true", nil)
				return printContextControlPlan(f, "use", args[0], item)
			}
			if err := authorizeContextControl(f, args[0], preChange, allowContextChange); err != nil {
				return err
			}
			var handle *mutationAuditHandle
			operationErr := mqgovctx.Update(func(locked *corectx.Config[mqgovctx.Context]) error {
				if err := ensureContextUsePolicyUnchanged(locked, args[0], preChange); err != nil {
					return err
				}
				handle, err = beginMutationAudit(f, mutationAuditSpec{
					Action:      "mq.ctx.use",
					ContextName: args[0],
					Context:     preChange.meta,
					Target:      audit.EventTarget{ResourceType: "context", Resource: args[0]},
					Metadata:    mutationPayloadMetadata("mq.ctx.use", []byte(args[0])),
				})
				if err != nil {
					return err
				}
				locked.CurrentContext = args[0]
				return nil
			})
			if handle != nil {
				if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
					return err
				}
			} else if operationErr != nil {
				return operationErr
			}
			return newPrinter(f).JSONData("ContextItem", map[string]string{"current": args[0]})
		},
	}
}

func ctxListCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List contexts",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := mqgovctx.Load()
			if err != nil {
				return err
			}
			names := make([]string, 0, len(cfg.Contexts))
			for name := range cfg.Contexts {
				names = append(names, name)
			}
			sort.Strings(names)
			items := make([]map[string]any, 0, len(names))
			rows := make([][]string, 0, len(names))
			for _, name := range names {
				item := cfg.Contexts[name]
				current := name == cfg.CurrentContext
				items = append(items, contextView(name, item, current, false))
				rows = append(rows, []string{name, fmt.Sprint(current), item.Backend, item.Cluster, item.Namespace, fmt.Sprint(item.Protected), fmt.Sprint(item.Password != "")})
			}
			if f.Output == "json" {
				return newPrinter(f).JSONList("ContextList", items, len(items), 1, len(items), false)
			}
			return newPrinter(f).Table([]string{"NAME", "CURRENT", "BACKEND", "CLUSTER", "NAMESPACE", "PROTECTED", "CREDENTIAL"}, rows)
		},
	}
}

func ctxCurrentCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Show current context",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			item, name, err := mqgovctx.Current()
			if err != nil {
				return err
			}
			view := contextView(name, *item, true, false)
			view["credentialBackends"] = credstore.Available()
			return newPrinter(f).JSONData("ContextItem", view)
		},
	}
}

func ctxDeleteCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"remove", "rm"},
		Short:   "Delete a context",
		Args:    cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cfg, err := mqgovctx.Load()
			if err != nil {
				return err
			}
			item, ok := cfg.Contexts[args[0]]
			if !ok {
				return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", args[0]), nil)
			}
			preChange, err := contextPreChangePolicy(cfg, args[0])
			if err != nil {
				return err
			}
			if contextPlanOnly(f) {
				appendContextAuditWarn(f, audit.EventType("ctx.delete"), item, audit.StatusSuccess, "ctx delete dryRun=true", nil)
				return printContextControlPlan(f, "delete", args[0], item)
			}
			if err := authorizeContextControl(f, args[0], preChange, allowContextDelete); err != nil {
				return err
			}
			var handle *mutationAuditHandle
			operationErr := mqgovctx.Update(func(locked *corectx.Config[mqgovctx.Context]) error {
				if err := ensureContextPolicyUnchanged(locked, args[0], preChange); err != nil {
					return err
				}
				if _, exists := locked.Contexts[args[0]]; !exists {
					return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", args[0]), nil)
				}
				handle, err = beginMutationAudit(f, mutationAuditSpec{
					Action:      "mq.ctx.delete",
					ContextName: args[0],
					Context:     preChange.meta,
					Target:      audit.EventTarget{ResourceType: "context", Resource: args[0]},
					Metadata:    mutationPayloadMetadata("mq.ctx.delete", []byte(args[0])),
				})
				if err != nil {
					return err
				}
				delete(locked.Contexts, args[0])
				if locked.CurrentContext == args[0] {
					locked.CurrentContext = ""
				}
				return nil
			})
			if handle != nil {
				if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
					return err
				}
			} else if operationErr != nil {
				return operationErr
			}
			return newPrinter(f).JSONData("ContextItem", map[string]string{"deleted": args[0]})
		},
	}
}

func ctxExportCmd(f *cliFlags) *cobra.Command {
	var opts ctxExportOptions
	cmd := &cobra.Command{
		Use:   "export [name]",
		Short: "Export a portable context document",
		Args: func(_ *cobra.Command, args []string) error {
			if opts.all {
				if len(args) != 0 {
					return apperrors.New(apperrors.CodeUsageError, "ctx export --all accepts no positional context", nil)
				}
				return nil
			}
			if len(args) != 1 {
				return apperrors.New(apperrors.CodeUsageError, "ctx export requires a context name or --all", nil)
			}
			return nil
		},
		RunE: func(_ *cobra.Command, args []string) error {
			name := ""
			if len(args) == 1 {
				name = args[0]
			}
			return runCtxExport(f, name, opts)
		},
	}
	cmd.Flags().BoolVar(&opts.includeCredentials, "include-credentials", false, "Include plaintext credentials when stored as plain-yaml")
	cmd.Flags().StringVar(&opts.outputFile, "output", "", "Write portable context YAML to a file")
	cmd.Flags().BoolVar(&opts.all, "all", false, "Export all contexts")
	return cmd
}

func ctxImportCmd(f *cliFlags) *cobra.Command {
	var opts ctxImportOptions
	cmd := &cobra.Command{
		Use:   "import -f <file>",
		Short: "Import a portable context document",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runCtxImport(f, opts)
		},
	}
	cmd.Flags().StringVarP(&opts.file, "file", "f", "", "Portable context document to import")
	cmd.Flags().StringVar(&opts.file, "input", "", "Portable context document to import")
	cmd.Flags().StringVar(&opts.rename, "rename", "", "Import a single context under a different name")
	cmd.Flags().BoolVar(&opts.force, "force", false, "Overwrite an existing context")
	cmd.Flags().BoolVar(&opts.force, "overwrite", false, "Overwrite an existing context")
	return cmd
}

func ctxRoleCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "role",
		Short: "Manage context RBAC roles",
		Args:  requireSubcommand,
		RunE:  runParentHelp,
	}
	cmd.AddCommand(ctxRoleSetCmd(f), ctxRoleUnsetCmd(f), ctxRoleListCmd(f))
	return cmd
}

func ctxRoleSetCmd(f *cliFlags) *cobra.Command {
	var opts roleOptions
	cmd := &cobra.Command{
		Use:   "set <context>",
		Short: "Assign an operator role for a context",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runCtxRoleSet(f, args[0], opts)
		},
	}
	cmd.Flags().StringVar(&opts.targetOperator, "target-operator", "", "Operator identity to assign")
	cmd.Flags().StringVar(&opts.role, "role", "", "Role: reader, writer, admin")
	return cmd
}

func ctxRoleUnsetCmd(f *cliFlags) *cobra.Command {
	var opts roleOptions
	cmd := &cobra.Command{
		Use:   "unset <context>",
		Short: "Remove an operator role from a context",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runCtxRoleUnset(f, args[0], opts)
		},
	}
	cmd.Flags().StringVar(&opts.targetOperator, "target-operator", "", "Operator identity to remove")
	return cmd
}

func ctxRoleListCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list <context>",
		Short: "List operator roles for a context",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runCtxRoleList(f, args[0])
		},
	}
}

func ctxMigrateCredentialsCmd(f *cliFlags) *cobra.Command {
	opts := migrateCredentialsOptions{toBackend: credentialBackendEncrypted}
	cmd := &cobra.Command{
		Use:   "migrate-credentials",
		Short: "Move literal context credentials to a secure credential backend",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runCtxMigrateCredentials(f, opts)
		},
	}
	cmd.Flags().StringVar(&opts.toBackend, "to", credentialBackendEncrypted, "Target backend: encrypted-file or keychain")
	cmd.Flags().StringVar(&opts.contextName, "context", "", "Context to migrate")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Preview credential migration without writing")
	return cmd
}

func ctxTestCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "test [name]",
		Short: "Test backend connectivity for a context",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctxName := f.contextName()
			if len(args) == 1 {
				f.Context = args[0]
				ctxName = args[0]
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				appendContextAuditWarn(f, audit.EventContextTest, mqgovctx.Context{}, audit.StatusFailed, "ctx test", err)
				return err
			}
			defer backend.Close()
			err = backend.Ping(cmd.Context())
			status := audit.StatusSuccess
			if err != nil {
				status = audit.StatusFailed
			}
			appendContextAuditWarn(f, audit.EventContextTest, meta, status, "backend="+backend.Describe().Backend, err)
			if err != nil {
				return err
			}
			return newPrinter(f).JSONData("ContextTestResult", map[string]any{"name": ctxName, "backend": backend.Describe().Backend, "ok": true})
		},
	}
}

func runCtxExport(f *cliFlags, name string, opts ctxExportOptions) error {
	cfg, err := mqgovctx.Load()
	if err != nil {
		return err
	}
	if opts.all {
		return runCtxExportAll(f, cfg.Contexts, opts)
	}
	item, ok := cfg.Contexts[name]
	if !ok {
		return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", name), nil)
	}
	if opts.includeCredentials {
		if item.CredentialBackend != "" && item.CredentialBackend != "plain-yaml" {
			return apperrors.New(apperrors.CodeCredentialStoreError, fmt.Sprintf("credentials backed by %s cannot be exported in cleartext", item.CredentialBackend), nil)
		}
	} else {
		redactContextCredentials(&item)
	}
	data, err := yaml.Marshal(contextExportDocument{APIVersion: ctxExportAPIVersion, Name: name, Context: &item})
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to marshal context export", err)
	}
	if opts.outputFile != "" {
		if err := validateContextExportPath(f, opts.outputFile); err != nil {
			return err
		}
	}
	if contextPlanOnly(f) {
		appendContextAuditWarn(f, audit.EventContextExport, item, audit.StatusSuccess, "ctx export dryRun=true", nil)
		return newPrinter(f).JSONData("ChangePlan", map[string]any{
			"resourceType": "file",
			"action":       "context export",
			"context":      name,
			"path":         opts.outputFile,
			"dryRun":       true,
		})
	}
	if opts.outputFile == "" {
		if err := writeContextExport(f, "", data); err != nil {
			return err
		}
		appendContextAuditWarn(f, audit.EventContextExport, item, audit.StatusSuccess, "ctx export stdout", nil)
		return nil
	}
	metadata := mutationPayloadMetadata("mq.ctx.export", data)
	metadata.Items = 1
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:      "mq.ctx.export",
		ContextName: name,
		Context:     item,
		Target:      audit.EventTarget{ResourceType: "file"},
		Metadata:    metadata,
	})
	if err != nil {
		return err
	}
	operationErr := writeContextExport(f, opts.outputFile, data)
	return finishMutationAudit(handle, mutationAuditOutcome{}, operationErr)
}

func runCtxExportAll(f *cliFlags, contexts map[string]mqgovctx.Context, opts ctxExportOptions) error {
	names := make([]string, 0, len(contexts))
	for name := range contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	exported := make(map[string]mqgovctx.Context, len(names))
	for _, name := range names {
		item := contexts[name]
		if opts.includeCredentials {
			if item.CredentialBackend != "" && item.CredentialBackend != "plain-yaml" {
				return apperrors.New(apperrors.CodeCredentialStoreError, fmt.Sprintf("credentials backed by %s cannot be exported in cleartext", item.CredentialBackend), nil)
			}
		} else {
			redactContextCredentials(&item)
		}
		exported[name] = item
	}
	data, err := yaml.Marshal(contextExportDocument{APIVersion: ctxExportAPIVersion, Contexts: exported})
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to marshal context export", err)
	}
	if opts.outputFile != "" {
		if err := validateContextExportPath(f, opts.outputFile); err != nil {
			return err
		}
	}
	if contextPlanOnly(f) {
		return newPrinter(f).JSONData("ChangePlan", map[string]any{
			"resourceType": "file",
			"action":       "context export",
			"contexts":     names,
			"count":        len(names),
			"path":         opts.outputFile,
			"dryRun":       true,
		})
	}
	if opts.outputFile == "" {
		if err := writeContextExport(f, "", data); err != nil {
			return err
		}
		appendAuditWarn(f, audit.EventContextExport, mqgovctx.Context{}, audit.EventTarget{ResourceType: "context"}, audit.StatusSuccess, "ctx export --all stdout", nil)
		return nil
	}
	metadata := mutationPayloadMetadata("mq.ctx.export-batch", data)
	metadata.Items = len(names)
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:   "mq.ctx.export-batch",
		Context:  mqgovctx.Context{},
		Target:   audit.EventTarget{ResourceType: "file"},
		Metadata: metadata,
	})
	if err != nil {
		return err
	}
	operationErr := writeContextExport(f, opts.outputFile, data)
	succeeded := 0
	failed := 0
	if operationErr == nil {
		succeeded = len(names)
	} else {
		failed = 1
	}
	return finishBatchMutationAudit(handle, len(names), succeeded, failed, operationErr)
}

func writeContextExport(f *cliFlags, path string, data []byte) error {
	if path == "" {
		if _, err := os.Stdout.Write(data); err != nil {
			return apperrors.New(apperrors.CodeLocalIOError, "failed to write context export", err)
		}
		return nil
	}
	if err := validateContextExportPath(f, path); err != nil {
		return err
	}
	parent := filepath.Dir(path)
	id, err := newMutationID(rand.Reader)
	if err != nil {
		return err
	}
	tempPath := filepath.Join(parent, ".mqgov-context-export-"+id+".tmp")
	file, err := openPrivateContextExportTemp(tempPath)
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to create private context export temporary file", nil)
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to write context export temporary file", nil)
	}
	if err := file.Sync(); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to sync context export temporary file", nil)
	}
	if err := file.Close(); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to close context export temporary file", nil)
	}
	if err := replaceContextExportFile(tempPath, path); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to atomically replace context export file", nil)
	}
	complete = true
	if err := verifyContextExportOwnerOnly(path); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "context export file is not owner-only", nil)
	}
	return nil
}

func validateContextExportPath(f *cliFlags, path string) error {
	target := strings.TrimSpace(path)
	if target == "" {
		return apperrors.New(apperrors.CodeUsageError, "context export output path is required", nil)
	}
	if err := validateContextExportTargetType(target); err != nil {
		return err
	}
	targetResolved, err := resolveContextExportAlias(target)
	if err != nil {
		return err
	}
	return rejectContextExportStateConflict(f, target, targetResolved)
}

func validateContextExportTargetType(target string) error {
	info, err := os.Lstat(target)
	if err == nil {
		reparse, reparseErr := contextExportPathIsReparse(target)
		if reparseErr != nil {
			return reparseErr
		}
		if info.Mode()&os.ModeSymlink != 0 || reparse {
			return apperrors.New(apperrors.CodeLocalIOError, "context export output must not be a symlink or reparse point", nil)
		}
		if !info.Mode().IsRegular() {
			return apperrors.New(apperrors.CodeLocalIOError, "context export output must be a regular file", nil)
		}
	} else if !os.IsNotExist(err) {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to inspect context export output", nil)
	}
	return nil
}

func rejectContextExportStateConflict(f *cliFlags, target, targetResolved string) error {
	protected, spoolPath, err := contextExportProtectedPaths(f)
	if err != nil {
		return err
	}
	for _, protectedPath := range protected {
		conflict, conflictErr := contextExportPathsConflict(target, targetResolved, protectedPath)
		if conflictErr != nil {
			return conflictErr
		}
		if conflict {
			return apperrors.New(apperrors.CodeUsageError, "context export output conflicts with governed state", nil)
		}
	}
	auditPath, err := audit.DefaultPath()
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to resolve mutation audit path", nil)
	}
	auditResolved, err := resolveContextExportAlias(auditPath)
	if err != nil {
		return err
	}
	if contextExportConflictsWithAuditTempNamespace(targetResolved, auditResolved) {
		return apperrors.New(apperrors.CodeUsageError, "context export output conflicts with temporary audit state", nil)
	}
	if _, rotatedName := audit.RotatedFileTimestamp(auditResolved, targetResolved); rotatedName {
		return apperrors.New(apperrors.CodeUsageError, "context export output conflicts with rotated audit state", nil)
	}
	spoolResolved, err := resolveContextExportAlias(spoolPath)
	if err != nil {
		return err
	}
	if contextExportPathWithin(targetResolved, spoolResolved) {
		return apperrors.New(apperrors.CodeUsageError, "context export output conflicts with the mutation audit spool", nil)
	}
	return nil
}

func contextExportConflictsWithAuditTempNamespace(target, auditPath string) bool {
	if !contextExportPathsEqual(filepath.Dir(target), filepath.Dir(auditPath)) {
		return false
	}
	targetName := filepath.Base(target)
	auditName := filepath.Base(auditPath)
	if filepath.VolumeName(target) != "" || filepath.VolumeName(auditPath) != "" {
		targetName = strings.ToLower(targetName)
		auditName = strings.ToLower(auditName)
	}
	return strings.HasPrefix(targetName, auditName+".checkpoint.tmp-") ||
		strings.HasPrefix(targetName, auditName+".hmac-key.tmp-")
}

func contextExportProtectedPaths(f *cliFlags) ([]string, string, error) {
	configPath, err := contextExportConfigPath(f)
	if err != nil {
		return nil, "", err
	}
	auditPath, err := audit.DefaultPath()
	if err != nil {
		return nil, "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve mutation audit path", nil)
	}
	spoolPath := mutationAuditSpoolPath(auditPath)
	paths := []string{
		configPath,
		configPath + ".tmp",
		configPath + ".lock",
		filepath.Join(filepath.Dir(configPath), "config.lock"),
		auditPath,
		auditPath + ".lock",
		auditPath + ".checkpoint",
		auditPath + ".hmac-key",
		spoolPath,
		filepath.Join(spoolPath, mutationAuditSpoolLockBase+".lock"),
	}
	rotated, err := audit.RotatedFiles(auditPath)
	if err != nil {
		return nil, "", err
	}
	paths = append(paths, rotated...)
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve credential store path", nil)
	}
	credentialPath := filepath.Join(home, ".mqgov-cli", "credentials.enc")
	paths = append(paths, credentialPath, credentialPath+".tmp")
	return paths, spoolPath, nil
}

func contextExportConfigPath(f *cliFlags) (string, error) {
	if f != nil && strings.TrimSpace(f.Config) != "" {
		return f.Config, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve context config path", nil)
	}
	return filepath.Join(home, ".mqgov-cli", "config.yaml"), nil
}

func resolveContextExportAlias(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve context export path", nil)
	}
	current := filepath.Clean(absolute)
	var suffix []string
	for {
		if _, err := os.Lstat(current); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return "", apperrors.New(apperrors.CodeLocalIOError, "failed to inspect context export path", nil)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", apperrors.New(apperrors.CodeLocalIOError, "context export path has no existing ancestor", nil)
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
	resolved, err := filepath.EvalSymlinks(current)
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve context export path aliases", nil)
	}
	for index := len(suffix) - 1; index >= 0; index-- {
		resolved = filepath.Join(resolved, suffix[index])
	}
	return filepath.Clean(resolved), nil
}

func contextExportPathsConflict(target, targetResolved, protected string) (bool, error) {
	protectedResolved, err := resolveContextExportAlias(protected)
	if err != nil {
		return false, err
	}
	if contextExportPathsEqual(targetResolved, protectedResolved) {
		return true, nil
	}
	targetInfo, targetErr := os.Stat(target)
	protectedInfo, protectedErr := os.Stat(protected)
	if targetErr == nil && protectedErr == nil && os.SameFile(targetInfo, protectedInfo) {
		return true, nil
	}
	if targetErr != nil && !os.IsNotExist(targetErr) {
		return false, apperrors.New(apperrors.CodeLocalIOError, "failed to identify context export output", nil)
	}
	if protectedErr != nil && !os.IsNotExist(protectedErr) {
		return false, apperrors.New(apperrors.CodeLocalIOError, "failed to identify governed state path", nil)
	}
	return false, nil
}

func contextExportPathsEqual(left, right string) bool {
	if filepath.VolumeName(left) != "" || filepath.VolumeName(right) != "" {
		return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func contextExportPathWithin(path, directory string) bool {
	relative, err := filepath.Rel(directory, path)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)))
}

func redactContextCredentials(item *mqgovctx.Context) {
	if item.Password != "" {
		item.Password = redactedCredential
	}
	if item.KafkaSchemaRegistryPassword != "" {
		item.KafkaSchemaRegistryPassword = redactedCredential
	}
	item.RabbitMQAMQPURL = redact.String(item.RabbitMQAMQPURL)
}

func runCtxImport(f *cliFlags, opts ctxImportOptions) error {
	if err := validateCtxImportOptions(f, opts); err != nil {
		return err
	}
	doc, err := readContextExportDocument(opts.file)
	if err != nil {
		return err
	}
	if len(doc.Contexts) > 0 {
		return runCtxImportMany(f, doc, opts)
	}
	return runCtxImportOne(f, doc, opts)
}

func validateCtxImportOptions(_ *cliFlags, opts ctxImportOptions) error {
	if opts.file == "" {
		return apperrors.New(apperrors.CodeUsageError, "-f/--file or --input is required", nil)
	}
	return nil
}

func runCtxImportOne(f *cliFlags, doc contextExportDocument, opts ctxImportOptions) error { //nolint:gocyclo // Validation, pre-change authorization, and locked apply form one transaction flow.
	if doc.Context == nil {
		return apperrors.New(apperrors.CodeUsageError, "context import file has no context", nil)
	}
	name := firstNonEmpty(opts.rename, doc.Name)
	if name == "" {
		return apperrors.New(apperrors.CodeUsageError, "context name is required", nil)
	}
	item := *doc.Context
	credentialRedacted := clearRedactedImportedCredentials(&item)
	candidate, err := planContextImportCredential(name, &item)
	if err != nil {
		return err
	}
	if err := validateContextDefinition(&item); err != nil {
		return err
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		return err
	}
	if _, exists := cfg.Contexts[name]; exists && !opts.force {
		return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q already exists; use --force to overwrite", name), nil)
	}
	preChange, err := contextPreChangePolicy(cfg, name)
	if err != nil {
		return err
	}
	if contextPlanOnly(f) {
		appendContextAuditWarn(f, audit.EventContextImport, preChange.meta, audit.StatusSuccess, "ctx import dryRun=true", nil)
		return newPrinter(f).JSONData("ChangePlan", map[string]any{
			"resourceType":       "context",
			"action":             "import",
			"context":            name,
			"credentialRedacted": credentialRedacted,
			"dryRun":             true,
		})
	}
	if err := authorizeContextControl(f, name, preChange, allowContextChange); err != nil {
		return err
	}
	candidates := []contextImportCredentialCandidate{candidate}
	if err := validateContextImportCredentialCandidates(candidates); err != nil {
		return err
	}
	metadata := mutationValueMetadata("mq.ctx.import", item)
	metadata.Items = 1
	expected := map[string]mqgovctx.Context{name: item}
	var handle *mutationAuditHandle
	var transaction *contextImportCredentialTransaction
	var original map[string]contextImportTargetState
	compensationStatus := ""
	compensationUncertain := false
	compensated := false
	importContext := f.commandCtx
	if importContext == nil {
		importContext = context.Background()
	}
	operationErr := mqgovctx.Update(func(locked *corectx.Config[mqgovctx.Context]) error {
		if err := ensureContextPolicyUnchanged(locked, name, preChange); err != nil {
			return err
		}
		if err := validateContextImportCredentialKeySet(locked, expected); err != nil {
			return err
		}
		original = captureContextImportTargets(locked, []string{name})
		handle, err = beginMutationAudit(f, mutationAuditSpec{
			Action:      "mq.ctx.import",
			ContextName: name,
			Context:     preChange.meta,
			Target:      audit.EventTarget{ResourceType: "context", Resource: name},
			Metadata:    metadata,
		})
		if err != nil {
			return err
		}
		transaction, err = storeContextImportCredentials(importContext, candidates)
		if err != nil {
			compensated = true
			var compensationErr error
			compensationStatus, compensationErr = compensateContextImportCredentialsLocked(importContext, locked, transaction)
			if compensationErr != nil {
				compensationUncertain = true
				return contextImportCompensationError(errors.Join(err, compensationErr))
			}
			return err
		}
		locked.Contexts[name] = item
		return nil
	})
	if operationErr != nil && transaction != nil && !compensated {
		var reconciliationErr error
		compensationStatus, compensationUncertain, reconciliationErr = reconcileContextImportFailure(
			importContext,
			transaction,
			original,
			expected,
		)
		if reconciliationErr != nil {
			operationErr = contextImportCompensationError(errors.Join(operationErr, reconciliationErr))
		}
	}
	if handle != nil {
		if err := finishContextImportAudit(handle, 1, compensationStatus, compensationUncertain, operationErr); err != nil {
			return err
		}
	} else if operationErr != nil {
		return operationErr
	}
	result := contextImportResult{Name: name, Names: []string{name}, Count: 1, CredentialRedacted: credentialRedacted}
	if f.Output == "json" {
		return newPrinter(f).JSONData("ContextImportResult", result)
	}
	p := newPrinter(f)
	if err := p.Success(fmt.Sprintf("context %q imported", name)); err != nil {
		return err
	}
	if credentialRedacted {
		return p.Info(fmt.Sprintf("credential is redacted; run: mqgov ctx set %s with a credential backend", name))
	}
	return nil
}

func runCtxImportMany(f *cliFlags, doc contextExportDocument, opts ctxImportOptions) error { //nolint:gocyclo // Validate-all, authorize-all, and locked apply intentionally remain one transaction flow.
	if opts.rename != "" {
		return apperrors.New(apperrors.CodeUsageError, "--rename cannot be used with multi-context import", nil)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		return err
	}
	names := make([]string, 0, len(doc.Contexts))
	for name := range doc.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	redacted := false
	preChanges := make(map[string]contextControlPolicy, len(names))
	candidates := make([]contextImportCredentialCandidate, 0, len(names))
	for _, name := range names {
		item := doc.Contexts[name]
		redacted = clearRedactedImportedCredentials(&item) || redacted
		candidate, err := planContextImportCredential(name, &item)
		if err != nil {
			return err
		}
		candidates = append(candidates, candidate)
		if err := validateContextDefinition(&item); err != nil {
			return err
		}
		if _, exists := cfg.Contexts[name]; exists && !opts.force {
			return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q already exists; use --overwrite to overwrite", name), nil)
		}
		doc.Contexts[name] = item
		preChange, err := contextPreChangePolicy(cfg, name)
		if err != nil {
			return err
		}
		preChanges[name] = preChange
	}
	if contextPlanOnly(f) {
		for _, name := range names {
			appendContextAuditWarn(f, audit.EventContextImport, preChanges[name].meta, audit.StatusSuccess, "ctx import --all dryRun=true", nil)
		}
		return newPrinter(f).JSONData("ChangePlan", map[string]any{
			"resourceType":       "context",
			"action":             "import",
			"contexts":           names,
			"count":              len(names),
			"credentialRedacted": redacted,
			"dryRun":             true,
		})
	}
	for _, name := range names {
		if err := authorizeContextControl(f, name, preChanges[name], allowContextChange); err != nil {
			return err
		}
	}
	if err := validateContextImportCredentialCandidates(candidates); err != nil {
		return err
	}
	auditContextName := "batch"
	auditContext := mqgovctx.Context{}
	if len(names) > 0 {
		auditContextName = firstNonEmpty(preChanges[names[0]].source, names[0])
		auditContext = preChanges[names[0]].meta
	}
	metadata := mutationValueMetadata("mq.ctx.import-batch", doc.Contexts)
	metadata.Items = len(names)
	var handle *mutationAuditHandle
	var transaction *contextImportCredentialTransaction
	var original map[string]contextImportTargetState
	compensationStatus := ""
	compensationUncertain := false
	compensated := false
	importContext := f.commandCtx
	if importContext == nil {
		importContext = context.Background()
	}
	operationErr := mqgovctx.Update(func(locked *corectx.Config[mqgovctx.Context]) error {
		for _, name := range names {
			if err := ensureContextPolicyUnchanged(locked, name, preChanges[name]); err != nil {
				return err
			}
		}
		if err := validateContextImportCredentialKeySet(locked, doc.Contexts); err != nil {
			return err
		}
		original = captureContextImportTargets(locked, names)
		handle, err = beginMutationAudit(f, mutationAuditSpec{
			Action:      "mq.ctx.import-batch",
			ContextName: auditContextName,
			Context:     auditContext,
			Target:      audit.EventTarget{ResourceType: "context"},
			Metadata:    metadata,
		})
		if err != nil {
			return err
		}
		transaction, err = storeContextImportCredentials(importContext, candidates)
		if err != nil {
			compensated = true
			var compensationErr error
			compensationStatus, compensationErr = compensateContextImportCredentialsLocked(importContext, locked, transaction)
			if compensationErr != nil {
				compensationUncertain = true
				return contextImportCompensationError(errors.Join(err, compensationErr))
			}
			return err
		}
		for _, name := range names {
			locked.Contexts[name] = doc.Contexts[name]
		}
		return nil
	})
	if operationErr != nil && transaction != nil && !compensated {
		var reconciliationErr error
		compensationStatus, compensationUncertain, reconciliationErr = reconcileContextImportFailure(
			importContext,
			transaction,
			original,
			doc.Contexts,
		)
		if reconciliationErr != nil {
			operationErr = contextImportCompensationError(errors.Join(operationErr, reconciliationErr))
		}
	}
	if handle != nil {
		if err := finishContextImportAudit(handle, len(names), compensationStatus, compensationUncertain, operationErr); err != nil {
			return err
		}
	} else if operationErr != nil {
		return operationErr
	}
	result := contextImportResult{Names: names, Count: len(names), CredentialRedacted: redacted}
	if f.Output == "json" {
		return newPrinter(f).JSONData("ContextImportResult", result)
	}
	return newPrinter(f).Success(fmt.Sprintf("imported %d context(s)", len(names)))
}

func readContextExportDocument(path string) (contextExportDocument, error) {
	clean := filepath.Clean(path)
	if clean == "." || clean == string(filepath.Separator) {
		return contextExportDocument{}, apperrors.New(apperrors.CodeUsageError, "invalid context import file", nil)
	}
	info, err := os.Stat(clean)
	if err != nil {
		return contextExportDocument{}, apperrors.New(apperrors.CodeLocalIOError, "failed to stat context import file", err)
	}
	if info.IsDir() {
		return contextExportDocument{}, apperrors.New(apperrors.CodeUsageError, "context import file is a directory", nil)
	}
	data, err := os.ReadFile(clean) //nolint:gosec // User supplied context import path.
	if err != nil {
		return contextExportDocument{}, apperrors.New(apperrors.CodeLocalIOError, "failed to read context import file", err)
	}
	var doc contextExportDocument
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return contextExportDocument{}, apperrors.New(apperrors.CodeUsageError, "failed to parse context import file", err)
	}
	if doc.APIVersion != ctxExportAPIVersion {
		return contextExportDocument{}, apperrors.New(apperrors.CodeUnsupportedProtocol, fmt.Sprintf("unsupported context export apiVersion %q", doc.APIVersion), nil)
	}
	return doc, nil
}

func clearRedactedImportedCredentials(item *mqgovctx.Context) bool {
	redacted := false
	if item.Password == redactedCredential {
		item.Password = ""
		redacted = true
	}
	if item.KafkaSchemaRegistryPassword == redactedCredential {
		item.KafkaSchemaRegistryPassword = ""
		redacted = true
	}
	return redacted
}

func credentialBackendForContext(item mqgovctx.Context) (credstore.Backend, error) {
	if item.CredentialBackend == "vault" {
		return credstore.NewVault(credstore.VaultConfig{Addr: item.VaultAddr, Path: item.VaultPath, RoleID: item.VaultRoleID, Namespace: item.VaultNamespace}), nil
	}
	return credstore.New(item.CredentialBackend)
}

func validateContextDefinition(item *mqgovctx.Context) error {
	if !supportedContextBackend(item.Backend) {
		return apperrors.New(apperrors.CodeNotImplemented, "backend is not supported", nil)
	}
	if item.Backend == "rocketmq" {
		item.Namespace = strings.TrimSpace(item.Namespace)
		if item.Namespace != "" {
			return apperrors.New(apperrors.CodeNotImplemented, "RocketMQ namespace is not supported consistently by the v2 admin client", nil)
		}
	}
	return nil
}

func runCtxRoleSet(f *cliFlags, contextName string, opts roleOptions) error { //nolint:gocyclo // Validate, authorize, locked recheck, audit intent, and apply form one security flow.
	if opts.targetOperator == "" {
		return apperrors.New(apperrors.CodeUsageError, "--target-operator is required", nil)
	}
	if !validRole(opts.role) {
		return apperrors.New(apperrors.CodeUsageError, "--role must be reader, writer, or admin", nil)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		return err
	}
	_, ok := cfg.Contexts[contextName]
	if !ok {
		return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", contextName), nil)
	}
	preChange, err := contextPreChangePolicy(cfg, contextName)
	if err != nil {
		return err
	}
	if contextPlanOnly(f) {
		appendRoleAuditWarn(f, audit.EventRoleAssign, contextName, preChange.meta, opts.targetOperator, opts.role)
		return newPrinter(f).JSONData("ChangePlan", map[string]any{
			"resourceType": "role",
			"action":       "set",
			"context":      contextName,
			"operator":     opts.targetOperator,
			"role":         opts.role,
			"dryRun":       true,
		})
	}
	if err := authorizeContextControl(f, contextName, preChange, allowRoleChange); err != nil {
		return err
	}
	metadata := mutationValueMetadata("mq.ctx.role.set", map[string]string{
		"operator": opts.targetOperator,
		"role":     opts.role,
	})
	metadata.Items = 1
	var handle *mutationAuditHandle
	operationErr := mqgovctx.Update(func(locked *corectx.Config[mqgovctx.Context]) error {
		if err := ensureContextPolicyUnchanged(locked, contextName, preChange); err != nil {
			return err
		}
		current, exists := locked.Contexts[contextName]
		if !exists {
			return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", contextName), nil)
		}
		handle, err = beginMutationAudit(f, mutationAuditSpec{
			Action:      "mq.ctx.role.set",
			ContextName: contextName,
			Context:     preChange.meta,
			Target:      audit.EventTarget{ResourceType: "role", Resource: opts.targetOperator},
			Metadata:    metadata,
		})
		if err != nil {
			return err
		}
		if current.Roles == nil {
			current.Roles = map[string]string{}
		}
		current.Roles[opts.targetOperator] = opts.role
		locked.Contexts[contextName] = current
		return nil
	})
	if handle != nil {
		if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
			return err
		}
	} else if operationErr != nil {
		return operationErr
	}
	if f.Output == "json" {
		return newPrinter(f).JSONData("ContextItem", map[string]any{"context": contextName, "operator": opts.targetOperator, "role": opts.role})
	}
	return newPrinter(f).Success(fmt.Sprintf("role %q assigned to %q in context %q", opts.role, opts.targetOperator, contextName))
}

func runCtxRoleUnset(f *cliFlags, contextName string, opts roleOptions) error { //nolint:gocyclo // Validate, authorize, locked recheck, audit intent, and apply form one security flow.
	if opts.targetOperator == "" {
		return apperrors.New(apperrors.CodeUsageError, "--target-operator is required", nil)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		return err
	}
	_, ok := cfg.Contexts[contextName]
	if !ok {
		return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", contextName), nil)
	}
	preChange, err := contextPreChangePolicy(cfg, contextName)
	if err != nil {
		return err
	}
	if contextPlanOnly(f) {
		appendRoleAuditWarn(f, audit.EventRoleRevoke, contextName, preChange.meta, opts.targetOperator, "")
		return newPrinter(f).JSONData("ChangePlan", map[string]any{
			"resourceType": "role",
			"action":       "unset",
			"context":      contextName,
			"operator":     opts.targetOperator,
			"dryRun":       true,
		})
	}
	if err := authorizeContextControl(f, contextName, preChange, allowRoleChange); err != nil {
		return err
	}
	metadata := mutationValueMetadata("mq.ctx.role.unset", map[string]string{"operator": opts.targetOperator})
	metadata.Items = 1
	var handle *mutationAuditHandle
	operationErr := mqgovctx.Update(func(locked *corectx.Config[mqgovctx.Context]) error {
		if err := ensureContextPolicyUnchanged(locked, contextName, preChange); err != nil {
			return err
		}
		current, exists := locked.Contexts[contextName]
		if !exists {
			return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", contextName), nil)
		}
		handle, err = beginMutationAudit(f, mutationAuditSpec{
			Action:      "mq.ctx.role.unset",
			ContextName: contextName,
			Context:     preChange.meta,
			Target:      audit.EventTarget{ResourceType: "role", Resource: opts.targetOperator},
			Metadata:    metadata,
		})
		if err != nil {
			return err
		}
		if current.Roles != nil {
			delete(current.Roles, opts.targetOperator)
			if len(current.Roles) == 0 {
				current.Roles = nil
			}
		}
		locked.Contexts[contextName] = current
		return nil
	})
	if handle != nil {
		if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
			return err
		}
	} else if operationErr != nil {
		return operationErr
	}
	if f.Output == "json" {
		return newPrinter(f).JSONData("ContextItem", map[string]any{"context": contextName, "operator": opts.targetOperator, "removed": true})
	}
	return newPrinter(f).Success(fmt.Sprintf("role removed from %q in context %q", opts.targetOperator, contextName))
}

func runCtxRoleList(f *cliFlags, contextName string) error {
	item, err := loadContextForRole(contextName)
	if err != nil {
		return err
	}
	items := roleItems(item.Roles)
	p := newPrinter(f)
	if f.Output == "json" {
		return p.JSONList("RoleList", items, len(items), 1, len(items), false)
	}
	if len(items) == 0 {
		return p.Info("(no roles assigned)")
	}
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.Operator, item.Role})
	}
	return p.Table([]string{"OPERATOR", "ROLE"}, rows)
}

func loadContextForRole(name string) (mqgovctx.Context, error) {
	cfg, err := mqgovctx.Load()
	if err != nil {
		return mqgovctx.Context{}, err
	}
	item, ok := cfg.Contexts[name]
	if !ok {
		return mqgovctx.Context{}, apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", name), nil)
	}
	return item, nil
}

func validRole(role string) bool {
	return role == safety.RoleReader || role == safety.RoleWriter || role == safety.RoleAdmin
}

func roleItems(roles map[string]string) []roleItem {
	operators := make([]string, 0, len(roles))
	for operator := range roles {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	items := make([]roleItem, 0, len(operators))
	for _, operator := range operators {
		items = append(items, roleItem{Operator: operator, Role: roles[operator]})
	}
	return items
}

func runCtxMigrateCredentials(f *cliFlags, opts migrateCredentialsOptions) error { //nolint:gocyclo // Candidate discovery, authorize-all, and locked apply form one security-sensitive flow.
	if err := validateCredentialMigrationOptions(f, opts); err != nil {
		return err
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		return err
	}
	candidates, err := credentialMigrationCandidates(cfg, opts.contextName)
	if err != nil {
		return err
	}
	if err := validateCredentialMigrationKeySet(cfg, candidates, opts.toBackend); err != nil {
		return err
	}
	preview := opts.dryRun || contextPlanOnly(f)
	result := credentialMigrationResult{
		DryRun:      preview,
		Backend:     opts.toBackend,
		Contexts:    credentialMigrationContextNames(candidates),
		Credentials: credentialMigrationCredentialCount(candidates),
	}
	if preview {
		for _, candidate := range candidates {
			appendCredentialMigrationAuditWarn(f, candidate.name, candidate.context, opts.toBackend, nil)
		}
		return printCredentialMigrationResult(f, result)
	}
	if len(candidates) == 0 {
		return printCredentialMigrationResult(f, result)
	}
	preChanges := make(map[string]contextControlPolicy, len(candidates))
	for _, candidate := range candidates {
		preChange, err := contextPreChangePolicy(cfg, candidate.name)
		if err != nil {
			return err
		}
		preChanges[candidate.name] = preChange
		if err := authorizeContextControl(f, candidate.name, preChange, allowContextChange); err != nil {
			return err
		}
	}
	backend, err := credentialMigrationBackend(opts.toBackend)
	if err != nil {
		return err
	}
	metadata := mutationValueMetadata("mq.ctx.credentials.migrate", map[string]any{
		"backend":     opts.toBackend,
		"contexts":    result.Contexts,
		"credentials": result.Credentials,
	})
	metadata.Items = result.Credentials
	metadata.Updates = len(candidates)
	var handle *mutationAuditHandle
	var progress credentialMigrationProgress
	var transaction *credentialMigrationTransaction
	compensated := false
	compensationStatus := ""
	migrationContext := f.commandCtx
	if migrationContext == nil {
		migrationContext = context.Background()
	}
	operationErr := mqgovctx.Update(func(locked *corectx.Config[mqgovctx.Context]) error {
		for _, candidate := range candidates {
			if err := ensureContextPolicyUnchanged(locked, candidate.name, preChanges[candidate.name]); err != nil {
				return err
			}
			current, exists := locked.Contexts[candidate.name]
			if !exists || !reflect.DeepEqual(current, candidate.context) {
				return apperrors.New(apperrors.CodeAuthorizationRequired, "context changed during credential migration; retry the command", nil)
			}
		}
		if err := validateCredentialMigrationKeySet(locked, candidates, opts.toBackend); err != nil {
			return err
		}
		handle, err = beginMutationAudit(f, mutationAuditSpec{
			Action:      "mq.ctx.credentials.migrate",
			ContextName: firstNonEmpty(preChanges[candidates[0].name].source, candidates[0].name),
			Context:     preChanges[candidates[0].name].meta,
			Target:      audit.EventTarget{ResourceType: "credential", Resource: opts.toBackend},
			Metadata:    metadata,
		})
		if err != nil {
			return err
		}
		var storeErr error
		transaction, progress, storeErr = storeCredentialMigrationCandidates(migrationContext, backend, candidates)
		if storeErr != nil {
			compensated = true
			var compensationErr error
			compensationStatus, compensationErr = compensateCredentialMigrationLocked(migrationContext, locked, transaction)
			if compensationErr != nil {
				progress = progress.afterIncompleteCompensation()
				return credentialMigrationCompensationError(storeErr, compensationErr)
			}
			if compensationStatus == credentialCompensationSucceeded {
				progress = progress.afterSuccessfulCompensation()
			}
			return storeErr
		}
		for _, candidate := range candidates {
			item := candidate.context
			if candidate.password != "" {
				item.Password = credstore.EncodeRef(opts.toBackend)
			}
			if candidate.schemaRegistryPassword != "" {
				item.KafkaSchemaRegistryPassword = credstore.EncodeRef(opts.toBackend)
			}
			item.CredentialBackend = opts.toBackend
			locked.Contexts[candidate.name] = item
		}
		return nil
	})
	if operationErr != nil && transaction != nil && !compensated {
		var reconciliationErr error
		progress, compensationStatus, reconciliationErr = reconcileCredentialMigrationFailure(
			migrationContext,
			transaction,
			progress,
			candidates,
			opts.toBackend,
		)
		if reconciliationErr != nil {
			operationErr = credentialMigrationCompensationError(operationErr, reconciliationErr)
		}
	}
	if handle != nil {
		if operationErr == nil {
			progress.succeeded = result.Credentials
			progress.failed = 0
		}
		if err := finishCredentialMigrationAudit(
			handle,
			result.Credentials,
			progress,
			compensationStatus,
			operationErr,
		); err != nil {
			return err
		}
	} else if operationErr != nil {
		return operationErr
	}
	return printCredentialMigrationResult(f, result)
}

func reconcileCredentialMigrationFailure(
	ctx context.Context,
	transaction *credentialMigrationTransaction,
	progress credentialMigrationProgress,
	candidates []migrateCredentialCandidate,
	backendName string,
) (credentialMigrationProgress, string, error) {
	committed := false
	unchanged := false
	compensationStatus := ""
	stateErr := withContextStoreLock(func(locked *corectx.Config[mqgovctx.Context]) error {
		committed, unchanged = credentialMigrationConfigState(locked, candidates, backendName)
		if committed {
			compensationStatus = credentialCompensationNotSafe
			return nil
		}
		if !unchanged {
			compensationStatus = credentialCompensationNotSafe
			progress = progress.afterIncompleteCompensation()
			return credentialMigrationCompensationError(nil, nil)
		}
		var err error
		compensationStatus, err = compensateCredentialMigrationLocked(ctx, locked, transaction)
		switch compensationStatus {
		case credentialCompensationSucceeded:
			progress = progress.afterSuccessfulCompensation()
		case credentialCompensationIncomplete, credentialCompensationNotSafe:
			progress = progress.afterIncompleteCompensation()
		}
		return err
	})
	if stateErr != nil {
		if compensationStatus == "" {
			compensationStatus = credentialCompensationNotSafe
			progress = progress.afterIncompleteCompensation()
		}
		return progress, compensationStatus, stateErr
	}
	if committed {
		return progress, credentialCompensationNotSafe, nil
	}
	return progress, compensationStatus, nil
}

func finishCredentialMigrationAudit(
	handle *mutationAuditHandle,
	total int,
	progress credentialMigrationProgress,
	compensationStatus string,
	operationErr error,
) error {
	skipped := total - progress.succeeded - progress.failed - progress.uncertain
	if skipped < 0 {
		skipped = 0
	}
	status := audit.StatusSuccess
	if operationErr != nil {
		status = audit.StatusFailed
		if progress.succeeded > 0 {
			status = audit.StatusPartialFailed
		}
	}
	return finishMutationAudit(handle, mutationAuditOutcome{
		Status:             status,
		Succeeded:          progress.succeeded,
		Failed:             progress.failed,
		Skipped:            skipped,
		Uncertain:          progress.uncertain,
		CompensationStatus: compensationStatus,
		counted:            true,
	}, operationErr)
}

func (progress credentialMigrationProgress) afterSuccessfulCompensation() credentialMigrationProgress {
	progress.failed += progress.succeeded
	progress.succeeded = 0
	return progress
}

func (progress credentialMigrationProgress) afterIncompleteCompensation() credentialMigrationProgress {
	progress.uncertain += progress.succeeded + progress.failed
	progress.succeeded = 0
	progress.failed = 0
	return progress
}

func credentialMigrationConfigState(
	cfg *corectx.Config[mqgovctx.Context],
	candidates []migrateCredentialCandidate,
	backendName string,
) (committed bool, unchanged bool) {
	committed = true
	unchanged = true
	for _, candidate := range candidates {
		current, exists := cfg.Contexts[candidate.name]
		if !exists {
			return false, false
		}
		expected := candidate.context
		if candidate.password != "" {
			expected.Password = credstore.EncodeRef(backendName)
		}
		if candidate.schemaRegistryPassword != "" {
			expected.KafkaSchemaRegistryPassword = credstore.EncodeRef(backendName)
		}
		expected.CredentialBackend = backendName
		committed = committed && reflect.DeepEqual(current, expected)
		unchanged = unchanged && reflect.DeepEqual(current, candidate.context)
	}
	return committed, unchanged
}

func credentialMigrationBackend(name string) (credstore.Backend, error) {
	backend, err := credstore.New(name)
	if err != nil {
		return nil, apperrors.New(apperrors.CodeUsageError, err.Error(), err)
	}
	if err := backend.Available(); err != nil {
		return nil, apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("backend %q not available", name), err)
	}
	return backend, nil
}

func storeCredentialMigrationCandidates(
	ctx context.Context,
	backend credstore.Backend,
	candidates []migrateCredentialCandidate,
) (*credentialMigrationTransaction, credentialMigrationProgress, error) {
	transaction := &credentialMigrationTransaction{backend: backend}
	var progress credentialMigrationProgress
	for _, candidate := range candidates {
		if candidate.password != "" {
			if err := transaction.put(ctx, candidate.name, candidate.name+"\x00primary", candidate.password); err != nil {
				progress.failed++
				return transaction, progress, apperrors.New(
					apperrors.CodeCredentialStoreError,
					fmt.Sprintf("store credential for context %q failed", candidate.name),
					err,
				)
			}
			progress.succeeded++
		}
		if candidate.schemaRegistryPassword != "" {
			if err := transaction.put(ctx, candidate.name+"/schema-registry", candidate.name+"\x00schema-registry", candidate.schemaRegistryPassword); err != nil {
				progress.failed++
				return transaction, progress, apperrors.New(
					apperrors.CodeCredentialStoreError,
					fmt.Sprintf("store schema registry credential for context %q failed", candidate.name),
					err,
				)
			}
			progress.succeeded++
		}
	}
	return transaction, progress, nil
}

func (transaction *credentialMigrationTransaction) put(ctx context.Context, key, owner, value string) error {
	previous, err := transaction.backend.Get(ctx, key)
	existed := err == nil
	if err != nil && !errors.Is(err, credstore.ErrNotFound) {
		return err
	}
	transaction.writes = append(transaction.writes, credentialMigrationWrite{
		key:      key,
		slot:     credentialPhysicalSlot{backendName: transaction.backend.Name(), key: key},
		owner:    owner,
		previous: previous,
		written:  value,
		existed:  existed,
	})
	writeIndex := len(transaction.writes) - 1
	if err := transaction.backend.Put(ctx, key, value); err != nil {
		return err
	}
	transaction.writes[writeIndex].putSucceeded = true
	return nil
}

func (transaction *credentialMigrationTransaction) compensationPlan(ctx context.Context) ([]bool, error) {
	if transaction == nil || transaction.backend == nil {
		return nil, nil
	}
	restore := make([]bool, len(transaction.writes))
	for index, write := range transaction.writes {
		needsRestore, err := credentialWriteNeedsRestore(
			ctx,
			transaction.backend,
			write.key,
			write.previous,
			write.written,
			write.existed,
			write.putSucceeded,
		)
		if err != nil {
			return nil, err
		}
		restore[index] = needsRestore
	}
	return restore, nil
}

func (transaction *credentialMigrationTransaction) compensate(ctx context.Context, restore []bool) error {
	if transaction == nil || transaction.backend == nil {
		return nil
	}
	var compensationErr error
	for index := len(transaction.writes) - 1; index >= 0; index-- {
		if index >= len(restore) || !restore[index] {
			continue
		}
		write := transaction.writes[index]
		var err error
		if write.existed {
			err = transaction.backend.Put(ctx, write.key, write.previous)
		} else {
			err = transaction.backend.Delete(ctx, write.key)
		}
		if err != nil && compensationErr == nil {
			compensationErr = err
		}
	}
	return compensationErr
}

func compensateCredentialMigration(
	ctx context.Context,
	transaction *credentialMigrationTransaction,
) error {
	_, err := compensateCredentialMigrationTransaction(ctx, transaction)
	return err
}

func compensateCredentialMigrationLocked(
	ctx context.Context,
	cfg *corectx.Config[mqgovctx.Context],
	transaction *credentialMigrationTransaction,
) (string, error) {
	if transaction == nil || len(transaction.writes) == 0 {
		return "", nil
	}
	if err := validateCredentialMigrationCompensationOwners(cfg, transaction); err != nil {
		return credentialCompensationNotSafe, err
	}
	return compensateCredentialMigrationTransaction(ctx, transaction)
}

func compensateCredentialMigrationTransaction(
	ctx context.Context,
	transaction *credentialMigrationTransaction,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	compensationContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		credentialCompensationLimit,
	)
	defer cancel()
	restore, err := transaction.compensationPlan(compensationContext)
	if err != nil {
		return credentialCompensationNotSafe, err
	}
	if err := transaction.compensate(compensationContext, restore); err != nil {
		return credentialCompensationIncomplete, err
	}
	return credentialCompensationSucceeded, nil
}

func validateCredentialMigrationCompensationOwners(
	cfg *corectx.Config[mqgovctx.Context],
	transaction *credentialMigrationTransaction,
) error {
	if cfg == nil {
		return apperrors.New(apperrors.CodeCredentialStoreError, "context state is unavailable for credential compensation", nil)
	}
	owners, err := contextCredentialSlotOwners(cfg.Contexts)
	if err != nil {
		return err
	}
	for _, write := range transaction.writes {
		if owner, exists := owners[write.slot]; exists && owner != write.owner {
			return apperrors.New(
				apperrors.CodeCredentialStoreError,
				"credential slot ownership changed before compensation; refusing to overwrite",
				nil,
			)
		}
	}
	return nil
}

func credentialMigrationCompensationError(operationErr, compensationErr error) error {
	return apperrors.New(
		apperrors.CodeCredentialStoreError,
		"credential migration failed and credential compensation could not be completed safely",
		errors.Join(operationErr, compensationErr),
	)
}

func validateCredentialMigrationOptions(f *cliFlags, opts migrateCredentialsOptions) error {
	if !validCredentialMigrationBackend(opts.toBackend) {
		return apperrors.New(apperrors.CodeUsageError, "--to must be encrypted-file or keychain", nil)
	}
	if opts.dryRun && f.Yes {
		return apperrors.New(apperrors.CodeUsageError, "ctx migrate-credentials accepts only one of --dry-run or --yes", nil)
	}
	return nil
}

func validCredentialMigrationBackend(name string) bool {
	return name == credentialBackendEncrypted || name == credentialBackendKeychain
}

func credentialMigrationCandidates(cfg *corectx.Config[mqgovctx.Context], contextName string) ([]migrateCredentialCandidate, error) {
	if contextName != "" {
		item, ok := cfg.Contexts[contextName]
		if !ok {
			return nil, apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", contextName), nil)
		}
		candidate := migrateCredentialCandidate{name: contextName, context: item}
		if isLiteralCredential(item.Password) {
			candidate.password = item.Password
		}
		if isLiteralCredential(item.KafkaSchemaRegistryPassword) {
			candidate.schemaRegistryPassword = item.KafkaSchemaRegistryPassword
		}
		if candidate.password != "" || candidate.schemaRegistryPassword != "" {
			return []migrateCredentialCandidate{candidate}, nil
		}
		return nil, nil
	}
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	candidates := make([]migrateCredentialCandidate, 0, len(names))
	for _, name := range names {
		item := cfg.Contexts[name]
		candidate := migrateCredentialCandidate{name: name, context: item}
		if isLiteralCredential(item.Password) {
			candidate.password = item.Password
		}
		if isLiteralCredential(item.KafkaSchemaRegistryPassword) {
			candidate.schemaRegistryPassword = item.KafkaSchemaRegistryPassword
		}
		if candidate.password != "" || candidate.schemaRegistryPassword != "" {
			candidates = append(candidates, candidate)
		}
	}
	return candidates, nil
}

func isLiteralCredential(value string) bool {
	return value != "" && value != redactedCredential && !credstore.ParseRef(value).IsRef
}

func credentialMigrationContextNames(candidates []migrateCredentialCandidate) []string {
	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		names = append(names, candidate.name)
	}
	return names
}

func credentialMigrationCredentialCount(candidates []migrateCredentialCandidate) int {
	count := 0
	for _, candidate := range candidates {
		if candidate.password != "" {
			count++
		}
		if candidate.schemaRegistryPassword != "" {
			count++
		}
	}
	return count
}

func validateCredentialMigrationKeySet(
	cfg *corectx.Config[mqgovctx.Context],
	candidates []migrateCredentialCandidate,
	backendName string,
) error {
	contexts := make(map[string]mqgovctx.Context, len(cfg.Contexts))
	for name, item := range cfg.Contexts {
		contexts[name] = item
	}
	for _, candidate := range candidates {
		item := candidate.context
		if candidate.password != "" {
			item.Password = credstore.EncodeRef(backendName)
		}
		if candidate.schemaRegistryPassword != "" {
			item.KafkaSchemaRegistryPassword = credstore.EncodeRef(backendName)
		}
		item.CredentialBackend = backendName
		contexts[candidate.name] = item
	}
	_, err := contextCredentialSlotOwners(contexts)
	return err
}

func printCredentialMigrationResult(f *cliFlags, result credentialMigrationResult) error {
	p := newPrinter(f)
	if f.Output == "json" {
		return p.JSONData("CredentialMigration", result)
	}
	action := "would migrate"
	if !result.DryRun {
		action = "migrated"
	}
	return p.Success(fmt.Sprintf("%s %d credential(s) in %d context(s) to %s", action, result.Credentials, len(result.Contexts), result.Backend))
}

func contextView(name string, item mqgovctx.Context, current, showSecrets bool) map[string]any {
	password := ""
	if showSecrets {
		password = item.Password
	}
	return map[string]any{
		"name":                           name,
		"current":                        current,
		"backend":                        item.Backend,
		"cluster":                        item.Cluster,
		"namespace":                      item.Namespace,
		"username":                       item.Username,
		"password":                       password,
		"passwordSet":                    item.Password != "",
		"protected":                      item.Protected,
		"credentialBackend":              item.CredentialBackend,
		"kafkaBrokers":                   item.KafkaBrokers,
		"kafkaSaslMechanism":             item.KafkaSASLMechanism,
		"kafkaTls":                       item.KafkaTLS,
		"kafkaSchemaRegistryUrl":         item.KafkaSchemaRegistryURL,
		"kafkaSchemaRegistryUsername":    item.KafkaSchemaRegistryUsername,
		"kafkaSchemaRegistryPasswordSet": item.KafkaSchemaRegistryPassword != "",
		"rabbitmqAmqpUrl":                item.RabbitMQAMQPURL,
		"rabbitmqManagementUrl":          item.RabbitMQManagementURL,
		"rabbitmqHost":                   item.RabbitMQHost,
		"rabbitmqPort":                   item.RabbitMQPort,
		"rabbitmqVhost":                  item.RabbitMQVHost,
		"rabbitmqTls":                    item.RabbitMQTLS,
		"pulsarServiceUrl":               item.PulsarServiceURL,
		"pulsarAdminUrl":                 item.PulsarAdminURL,
		"pulsarTenant":                   item.PulsarTenant,
		"pulsarNamespace":                item.PulsarNamespace,
		"pulsarTls":                      item.PulsarTLS,
		"rocketmqNameServers":            item.RocketMQNameServers,
		"rocketmqBrokerAddr":             item.RocketMQBrokerAddr,
		"rocketmqAccessKey":              item.RocketMQAccessKey,
		"rocketmqTls":                    item.RocketMQTLS,
		"caCertFilesConfigured":          tlsCAConfigured(item),
		"clientCertsConfigured":          tlsClientConfigured(item),
	}
}

func tlsCAConfigured(item mqgovctx.Context) bool {
	return item.KafkaCACertFile != "" || item.RabbitMQCACertFile != "" || item.PulsarCACertFile != "" || item.RocketMQCACertFile != ""
}

func tlsClientConfigured(item mqgovctx.Context) bool {
	return item.KafkaClientCertFile != "" || item.KafkaClientKeyFile != "" ||
		item.RabbitMQClientCertFile != "" || item.RabbitMQClientKeyFile != "" ||
		item.PulsarClientCertFile != "" || item.PulsarClientKeyFile != "" ||
		item.RocketMQClientCertFile != "" || item.RocketMQClientKeyFile != ""
}

func appendContextAuditWarn(f *cliFlags, eventType audit.EventType, item mqgovctx.Context, status, diff string, err error) {
	appendAuditWarn(f, eventType, item, audit.EventTarget{ResourceType: "context", Resource: f.contextName()}, status, diff, err)
}

func appendRoleAuditWarn(f *cliFlags, eventType audit.EventType, contextName string, item mqgovctx.Context, operator, role string) {
	path, pathErr := audit.DefaultPath()
	if pathErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: audit path failed: %v\n", pathErr)
		return
	}
	record := newSafeAuditRecord(
		f,
		eventType,
		item,
		contextName,
		audit.EventTarget{ResourceType: "role", Resource: operator},
		audit.StatusSuccess,
		"",
		nil,
	)
	record.RoleChange = &audit.EventRoleChange{
		ChangedOperator: operator,
		Role:            role,
	}
	if appendErr := appendQueuedAuditRecord(f, path, record); appendErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: audit write failed: %v\n", appendErr)
	}
}

func appendCredentialMigrationAuditWarn(f *cliFlags, contextName string, item mqgovctx.Context, backendName string, err error) {
	path, pathErr := audit.DefaultPath()
	if pathErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: audit path failed: %v\n", pathErr)
		return
	}
	status := audit.StatusSuccess
	if err != nil {
		status = audit.StatusFailed
	}
	record := newSafeAuditRecord(
		f,
		credentialMigrationEventType,
		item,
		contextName,
		audit.EventTarget{ResourceType: "credential", Resource: backendName},
		status,
		"credential backend="+backendName,
		err,
	)
	if appendErr := appendQueuedAuditRecord(f, path, record); appendErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: audit write failed: %v\n", appendErr)
	}
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
