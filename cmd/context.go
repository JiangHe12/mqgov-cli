package cmd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/audit"
	"github.com/JiangHe12/opskit-core/credstore"
	corectx "github.com/JiangHe12/opskit-core/ctx"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
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

func newContextCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "ctx", Aliases: []string{"context"}, Short: "Manage mqgov contexts", Args: requireSubcommand, RunE: runParentHelp}
	cmd.AddCommand(ctxSetCmd(f), ctxUseCmd(f), ctxListCmd(f), ctxCurrentCmd(f), ctxDeleteCmd(f), ctxTestCmd(f))
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
			if (opts.password != "" || opts.kafkaSchemaRegistryPassword != "") && (opts.credentialBackend == "" || opts.credentialBackend == "plain-yaml") {
				return apperrors.New(apperrors.CodeUsageError, "credentials must use a non-plain credential backend", nil)
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
