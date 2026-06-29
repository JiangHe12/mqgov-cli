package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/audit"
	"github.com/JiangHe12/opskit-core/credstore"
	corectx "github.com/JiangHe12/opskit-core/ctx"
	"github.com/JiangHe12/opskit-core/redact"
	"github.com/JiangHe12/opskit-core/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

const (
	ctxExportAPIVersion          = "mqgov-cli.io/ctx-export/v1"
	redactedCredential           = "<REDACTED>"
	credentialBackendEncrypted   = "encrypted-file"
	credentialBackendKeychain    = "keychain"
	credentialMigrationEventType = audit.EventType("credential.migrate")
)

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
					Username:          f.Operator,
					Protected:         opts.protected,
					CredentialBackend: opts.credentialBackend,
					OTLPRedact:        true,
				},
				Backend:   f.Backend,
				Cluster:   firstNonEmpty(opts.cluster, f.Cluster),
				Namespace: firstNonEmpty(opts.namespace, f.Namespace),
			}
			applyBackendContextOptions(&item, opts)
			var err error
			if opts.password != "" {
				item, err = mqgovctx.StoreCredential(cmd.Context(), args[0], opts.credentialBackend, opts.password, item)
				if err != nil {
					return apperrors.New(apperrors.CodeCredentialStoreError, "failed to store credential", err)
				}
			}
			item, err = mqgovctx.StoreKafkaSchemaRegistryCredential(cmd.Context(), args[0], opts.credentialBackend, opts.kafkaSchemaRegistryPassword, item)
			if err != nil {
				return apperrors.New(apperrors.CodeCredentialStoreError, "failed to store schema registry credential", err)
			}
			if err := mqgovctx.Set(args[0], item); err != nil {
				return err
			}
			appendContextAuditWarn(f, audit.EventType("ctx.set"), item, audit.StatusSuccess, "ctx set", nil)
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
	cmd.Flags().StringVar(&opts.rabbitUsername, "username", "", "RabbitMQ username")
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
			if err := mqgovctx.Use(args[0]); err != nil {
				return err
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
			newPrinter(f).Table([]string{"NAME", "CURRENT", "BACKEND", "CLUSTER", "NAMESPACE", "PROTECTED", "CREDENTIAL"}, rows)
			return nil
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
			item := cfg.Contexts[args[0]]
			if err := mqgovctx.Delete(args[0]); err != nil {
				return err
			}
			appendContextAuditWarn(f, audit.EventType("ctx.delete"), item, audit.StatusSuccess, "ctx delete", nil)
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
	if err := writeContextExport(opts.outputFile, data); err != nil {
		return err
	}
	appendContextAuditWarn(f, audit.EventContextExport, item, audit.StatusSuccess, "ctx export", nil)
	return nil
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
		appendContextAuditWarn(f, audit.EventContextExport, item, audit.StatusSuccess, "ctx export --all", nil)
	}
	data, err := yaml.Marshal(contextExportDocument{APIVersion: ctxExportAPIVersion, Contexts: exported})
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to marshal context export", err)
	}
	return writeContextExport(opts.outputFile, data)
}

func writeContextExport(path string, data []byte) error {
	if path != "" {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return apperrors.New(apperrors.CodeLocalIOError, "failed to write context export file", err)
		}
		return nil
	}
	if _, err := os.Stdout.Write(data); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to write context export", err)
	}
	return nil
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

func validateCtxImportOptions(f *cliFlags, opts ctxImportOptions) error {
	if f.NonInter && !f.Yes {
		return apperrors.New(apperrors.CodeAuthorizationRequired, "ctx import requires --yes in non-interactive mode", nil)
	}
	if opts.file == "" {
		return apperrors.New(apperrors.CodeUsageError, "-f/--file or --input is required", nil)
	}
	return nil
}

func runCtxImportOne(f *cliFlags, doc contextExportDocument, opts ctxImportOptions) error {
	if doc.Context == nil {
		return apperrors.New(apperrors.CodeUsageError, "context import file has no context", nil)
	}
	name := firstNonEmpty(opts.rename, doc.Name)
	if name == "" {
		return apperrors.New(apperrors.CodeUsageError, "context name is required", nil)
	}
	item := *doc.Context
	credentialRedacted := clearRedactedImportedCredentials(&item)
	if err := prepareImportedCredential(context.Background(), name, &item); err != nil {
		return err
	}
	if err := validateImportedContext(item); err != nil {
		return err
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		return err
	}
	if _, exists := cfg.Contexts[name]; exists && !opts.force {
		return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q already exists; use --force to overwrite", name), nil)
	}
	if err := mqgovctx.Set(name, item); err != nil {
		return err
	}
	appendContextAuditWarn(f, audit.EventContextImport, item, audit.StatusSuccess, "ctx import", nil)
	result := contextImportResult{Name: name, Names: []string{name}, Count: 1, CredentialRedacted: credentialRedacted}
	if f.Output == "json" {
		return newPrinter(f).JSONData("ContextImportResult", result)
	}
	p := newPrinter(f)
	p.Success(fmt.Sprintf("context %q imported", name))
	if credentialRedacted {
		p.Info(fmt.Sprintf("credential is redacted; run: mqgov ctx set %s with a credential backend", name))
	}
	return nil
}

func runCtxImportMany(f *cliFlags, doc contextExportDocument, opts ctxImportOptions) error {
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
	for _, name := range names {
		item := doc.Contexts[name]
		redacted = clearRedactedImportedCredentials(&item) || redacted
		if err := prepareImportedCredential(context.Background(), name, &item); err != nil {
			return err
		}
		if err := validateImportedContext(item); err != nil {
			return err
		}
		if _, exists := cfg.Contexts[name]; exists && !opts.force {
			return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q already exists; use --overwrite to overwrite", name), nil)
		}
		if err := mqgovctx.Set(name, item); err != nil {
			return err
		}
		appendContextAuditWarn(f, audit.EventContextImport, item, audit.StatusSuccess, "ctx import --all", nil)
	}
	result := contextImportResult{Names: names, Count: len(names), CredentialRedacted: redacted}
	if f.Output == "json" {
		return newPrinter(f).JSONData("ContextImportResult", result)
	}
	newPrinter(f).Success(fmt.Sprintf("imported %d context(s)", len(names)))
	return nil
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

func prepareImportedCredential(ctx context.Context, name string, item *mqgovctx.Context) error {
	if item.CredentialBackend == "" {
		if ref := credstore.ParseRef(item.Password); ref.IsRef {
			item.CredentialBackend = ref.BackendName
		}
		if ref := credstore.ParseRef(item.KafkaSchemaRegistryPassword); ref.IsRef {
			item.CredentialBackend = ref.BackendName
		}
	}
	if item.CredentialBackend == "" || item.CredentialBackend == "plain-yaml" {
		return nil
	}
	backend, err := credentialBackendForContext(*item)
	if err != nil {
		return err
	}
	if isLiteralCredential(item.Password) {
		if err := backend.Put(ctx, name, item.Password); err != nil {
			return apperrors.New(apperrors.CodeCredentialStoreError, "failed to store credential", err)
		}
		item.Password = credstore.EncodeRef(item.CredentialBackend)
	}
	if isLiteralCredential(item.KafkaSchemaRegistryPassword) {
		if err := backend.Put(ctx, name+"/schema-registry", item.KafkaSchemaRegistryPassword); err != nil {
			return apperrors.New(apperrors.CodeCredentialStoreError, "failed to store schema registry credential", err)
		}
		item.KafkaSchemaRegistryPassword = credstore.EncodeRef(item.CredentialBackend)
	}
	return nil
}

func credentialBackendForContext(item mqgovctx.Context) (credstore.Backend, error) {
	if item.CredentialBackend == "vault" {
		return credstore.NewVault(credstore.VaultConfig{Addr: item.VaultAddr, Path: item.VaultPath, RoleID: item.VaultRoleID, Namespace: item.VaultNamespace}), nil
	}
	return credstore.New(item.CredentialBackend)
}

func validateImportedContext(item mqgovctx.Context) error {
	if !supportedContextBackend(item.Backend) {
		return apperrors.New(apperrors.CodeNotImplemented, "backend is not supported", nil)
	}
	return nil
}

func runCtxRoleSet(f *cliFlags, contextName string, opts roleOptions) error {
	if opts.targetOperator == "" {
		return apperrors.New(apperrors.CodeUsageError, "--target-operator is required", nil)
	}
	if !validRole(opts.role) {
		return apperrors.New(apperrors.CodeUsageError, "--role must be reader, writer, or admin", nil)
	}
	item, err := loadContextForRole(contextName)
	if err != nil {
		return err
	}
	if item.Roles == nil {
		item.Roles = map[string]string{}
	}
	item.Roles[opts.targetOperator] = opts.role
	if err := mqgovctx.Set(contextName, item); err != nil {
		return err
	}
	appendRoleAuditWarn(f, audit.EventRoleAssign, contextName, item, opts.targetOperator, opts.role, nil)
	if f.Output == "json" {
		return newPrinter(f).JSONData("ContextItem", map[string]any{"context": contextName, "operator": opts.targetOperator, "role": opts.role})
	}
	newPrinter(f).Success(fmt.Sprintf("role %q assigned to %q in context %q", opts.role, opts.targetOperator, contextName))
	return nil
}

func runCtxRoleUnset(f *cliFlags, contextName string, opts roleOptions) error {
	if opts.targetOperator == "" {
		return apperrors.New(apperrors.CodeUsageError, "--target-operator is required", nil)
	}
	item, err := loadContextForRole(contextName)
	if err != nil {
		return err
	}
	if item.Roles != nil {
		delete(item.Roles, opts.targetOperator)
		if len(item.Roles) == 0 {
			item.Roles = nil
		}
	}
	if err := mqgovctx.Set(contextName, item); err != nil {
		return err
	}
	appendRoleAuditWarn(f, audit.EventRoleRevoke, contextName, item, opts.targetOperator, "", nil)
	if f.Output == "json" {
		return newPrinter(f).JSONData("ContextItem", map[string]any{"context": contextName, "operator": opts.targetOperator, "removed": true})
	}
	newPrinter(f).Success(fmt.Sprintf("role removed from %q in context %q", opts.targetOperator, contextName))
	return nil
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
		p.Info("(no roles assigned)")
		return nil
	}
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.Operator, item.Role})
	}
	p.Table([]string{"OPERATOR", "ROLE"}, rows)
	return nil
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

func runCtxMigrateCredentials(f *cliFlags, opts migrateCredentialsOptions) error {
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
	result := credentialMigrationResult{
		DryRun:      opts.dryRun,
		Backend:     opts.toBackend,
		Contexts:    credentialMigrationContextNames(candidates),
		Credentials: credentialMigrationCredentialCount(candidates),
	}
	if opts.dryRun || len(candidates) == 0 {
		return printCredentialMigrationResult(f, result)
	}
	backend, err := credentialMigrationBackend(opts.toBackend)
	if err != nil {
		return err
	}
	if err := storeCredentialMigrationCandidates(backend, candidates); err != nil {
		return err
	}
	if err := applyCredentialMigrationCandidates(f, opts.toBackend, candidates); err != nil {
		return err
	}
	return printCredentialMigrationResult(f, result)
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

func storeCredentialMigrationCandidates(backend credstore.Backend, candidates []migrateCredentialCandidate) error {
	for _, candidate := range candidates {
		if candidate.password != "" {
			if err := backend.Put(context.Background(), candidate.name, candidate.password); err != nil {
				return apperrors.New(apperrors.CodeCredentialStoreError, fmt.Sprintf("store credential for context %q failed", candidate.name), err)
			}
		}
		if candidate.schemaRegistryPassword != "" {
			if err := backend.Put(context.Background(), candidate.name+"/schema-registry", candidate.schemaRegistryPassword); err != nil {
				return apperrors.New(apperrors.CodeCredentialStoreError, fmt.Sprintf("store schema registry credential for context %q failed", candidate.name), err)
			}
		}
	}
	return nil
}

func applyCredentialMigrationCandidates(f *cliFlags, backendName string, candidates []migrateCredentialCandidate) error {
	for _, candidate := range candidates {
		item := candidate.context
		if candidate.password != "" {
			item.Password = credstore.EncodeRef(backendName)
		}
		if candidate.schemaRegistryPassword != "" {
			item.KafkaSchemaRegistryPassword = credstore.EncodeRef(backendName)
		}
		item.CredentialBackend = backendName
		if err := mqgovctx.Set(candidate.name, item); err != nil {
			appendCredentialMigrationAuditWarn(f, candidate.name, item, backendName, err)
			return err
		}
		appendCredentialMigrationAuditWarn(f, candidate.name, item, backendName, nil)
	}
	return nil
}

func validateCredentialMigrationOptions(f *cliFlags, opts migrateCredentialsOptions) error {
	if !validCredentialMigrationBackend(opts.toBackend) {
		return apperrors.New(apperrors.CodeUsageError, "--to must be encrypted-file or keychain", nil)
	}
	if opts.dryRun && f.Yes {
		return apperrors.New(apperrors.CodeUsageError, "ctx migrate-credentials accepts only one of --dry-run or --yes", nil)
	}
	if !opts.dryRun && !f.Yes {
		return apperrors.New(apperrors.CodeAuthorizationRequired, "ctx migrate-credentials requires --dry-run or --yes", nil)
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

func printCredentialMigrationResult(f *cliFlags, result credentialMigrationResult) error {
	p := newPrinter(f)
	if f.Output == "json" {
		return p.JSONData("CredentialMigration", result)
	}
	action := "would migrate"
	if !result.DryRun {
		action = "migrated"
	}
	p.Success(fmt.Sprintf("%s %d credential(s) in %d context(s) to %s", action, result.Credentials, len(result.Contexts), result.Backend))
	return nil
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

func appendRoleAuditWarn(f *cliFlags, eventType audit.EventType, contextName string, item mqgovctx.Context, operator, role string, err error) {
	path, pathErr := audit.DefaultPath()
	if pathErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: audit path failed: %v\n", pathErr)
		return
	}
	status := audit.StatusSuccess
	if err != nil {
		status = audit.StatusFailed
	}
	evt := audit.Event{
		EventType: eventType,
		Operator:  currentOperator(f),
		Context:   audit.EventContext{Name: contextName, Env: item.Env, Protected: item.Protected},
		Target:    audit.EventTarget{ResourceType: "role", Resource: operator},
		Status:    status,
		RoleChange: &audit.EventRoleChange{
			ChangedOperator: operator,
			Role:            role,
		},
	}
	if err != nil {
		appErr := apperrors.AsAppError(err)
		evt.Error = &audit.EventError{Code: string(appErr.Code), Message: appErr.Message}
	}
	if appendErr := audit.AppendWithOptions(path, evt, auditOptions(f)); appendErr != nil {
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
	evt := audit.Event{
		EventType: credentialMigrationEventType,
		Operator:  currentOperator(f),
		Context:   audit.EventContext{Name: contextName, Env: item.Env, Protected: item.Protected},
		Target:    audit.EventTarget{ResourceType: "credential", Resource: backendName},
		Status:    status,
		Diff:      "credential backend=" + backendName,
	}
	if err != nil {
		appErr := apperrors.AsAppError(err)
		evt.Error = &audit.EventError{Code: string(appErr.Code), Message: appErr.Message}
	}
	if appendErr := audit.AppendWithOptions(path, evt, auditOptions(f)); appendErr != nil {
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
