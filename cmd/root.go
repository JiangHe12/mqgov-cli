package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	osuser "os/user"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/audit"
	"github.com/JiangHe12/opskit-core/printer"
	"github.com/JiangHe12/opskit-core/redact"
	"github.com/JiangHe12/opskit-core/safety"
	"github.com/JiangHe12/opskit-core/telemetry"

	"github.com/JiangHe12/mqgov-cli/internal/backend/fake"
	kafkabackend "github.com/JiangHe12/mqgov-cli/internal/backend/kafka"
	pulsarbackend "github.com/JiangHe12/mqgov-cli/internal/backend/pulsar"
	rabbitmqbackend "github.com/JiangHe12/mqgov-cli/internal/backend/rabbitmq"
	rocketmqbackend "github.com/JiangHe12/mqgov-cli/internal/backend/rocketmq"
	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

const (
	apiVersion            = "mqgov-cli.io/v1"
	auditAPIVersion       = "mqgov-cli.io/audit/v1"
	allowOffsetReset      = safety.AllowFlag("allow-offset-reset")
	allowTopicPurge       = safety.AllowFlag("allow-topic-purge")
	allowTopicDelete      = safety.AllowFlag("allow-topic-delete")
	allowDestructiveACL   = safety.AllowFlag("allow-destructive-acl")
	allowInternalProduce  = safety.AllowFlag("allow-internal-produce")
	allowSchemaDelete     = safety.AllowFlag("allow-schema-delete")
	auditEventTopic       = audit.EventType("mq.topic")
	auditEventGroup       = audit.EventType("mq.group")
	auditEventMessage     = audit.EventType("mq.message")
	auditEventDLQ         = audit.EventType("mq.dlq")
	auditEventOffset      = audit.EventType("mq.offset")
	auditEventACL         = audit.EventType("mq.acl")
	auditEventSchema      = audit.EventType("mq.schema")
	auditEventFleet       = audit.EventType("mq.fleet")
	auditEventDiagnostic  = audit.EventType("mq.diagnostic")
	defaultFakeBackend    = "fake"
	defaultCommandTimeout = 30 * time.Second
)

type cliFlags struct {
	Config              string
	Context             string
	Backend             string
	Cluster             string
	Namespace           string
	Timeout             time.Duration
	Output              string
	PlainHead           bool
	Debug               bool
	Trace               bool
	NoColor             bool
	AuditMaxSize        int64
	DryRun              bool
	Plan                bool
	Yes                 bool
	Ticket              string
	Operator            string
	Reason              string
	NonInter            bool
	AllowOffsetReset    bool
	AllowTopicPurge     bool
	AllowTopicDelete    bool
	AllowDestructiveACL bool
	AllowInternalProd   bool
	AllowSchemaDelete   bool
	OTLPEnd             string
	OTLPMetrics         string
	OTLPInsec           bool
	contextOnce         sync.Once
	cachedCtx           string
	commandCtx          context.Context
	commandName         string
	commandTime         time.Time
	activeSpan          trace.Span
	telemetryStop       telemetry.ShutdownFunc
	metricsStop         telemetry.ShutdownFunc
	metricAttrs         []attribute.KeyValue
}

var versionInfo = struct {
	sync.RWMutex
	version string
	commit  string
	built   string
}{version: "dev", commit: "unknown", built: "unknown"}

func init() {
	mqgovctx.Configure()
	apperrors.Configure(apperrors.Options{APIVersion: apiVersion})
	printer.Configure(printer.Options{APIVersion: apiVersion, JSONEnvelopeByDefault: true})
	audit.Configure(audit.Config{APIVersion: auditAPIVersion, ConfigDirName: ".mqgov-cli", PrivateKeyEnvVar: "MQGOV_CLI_AUDIT_PRIVATE_KEY"})
	safety.Configure(safety.Config{Prompt: "Proceed with mqgov write? [y/N] ", OperatorEnvVar: "MQGOV_CLI_OPERATOR"})
	telemetry.Configure(telemetry.Config{ServiceName: "mqgov-cli", AttributePrefix: "mqgov", MetricNamePrefix: "mqgov", DomainAttributeName: "resource"})
}

func SetVersionInfo(version, commit, built string) {
	versionInfo.Lock()
	defer versionInfo.Unlock()
	versionInfo.version = version
	versionInfo.commit = commit
	versionInfo.built = built
}

func getVersionInfo() (string, string, string) {
	versionInfo.RLock()
	defer versionInfo.RUnlock()
	return versionInfo.version, versionInfo.commit, versionInfo.built
}

func newDefaultFlags() *cliFlags {
	return &cliFlags{Timeout: defaultCommandTimeout, Output: "table", AuditMaxSize: audit.DefaultMaxSizeBytes}
}

func NewRootCmd() *cobra.Command {
	return newRootCmdWith(newDefaultFlags())
}

func newRootCmdWith(f *cliFlags) *cobra.Command {
	v, _, _ := getVersionInfo()
	cmd := &cobra.Command{
		Use:           "mqgov-cli",
		Short:         "Governed message middleware CLI",
		Version:       v,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(c *cobra.Command, _ []string) error {
			applyGlobalFlags(f)
			f.commandCtx = c.Context()
			f.commandName = strings.ReplaceAll(c.CommandPath(), " ", ".")
			f.commandTime = time.Now()
			if f.Config != "" {
				mqgovctx.SetConfigPath(f.Config)
			}
			if err := validateOutput(f.Output); err != nil {
				return err
			}
			traceEndpoint, metricsEndpoint, insecure, ctxMeta, ctxName := resolveTelemetryConfig(f)
			f.telemetryStop = telemetry.Init(c.Context(), traceEndpoint, insecure, v)
			f.metricsStop = telemetry.InitMetrics(c.Context(), metricsEndpoint, insecure, v)
			spanCtx, span := telemetry.Tracer().Start(c.Context(), f.commandName)
			f.metricAttrs = telemetry.SpanAttributes(currentOperator(f), ctxName, ctxMeta.Env, "", f.Ticket, ctxMeta.Protected, true, "")
			span.SetAttributes(f.metricAttrs...)
			f.commandCtx = spanCtx
			f.activeSpan = span
			return nil
		},
	}
	cmd.PersistentFlags().StringVar(&f.Config, "config", "", "Override context config path")
	cmd.PersistentFlags().StringVar(&f.Context, "context", "", "Temporarily use a named context for this command")
	cmd.PersistentFlags().StringVar(&f.Backend, "backend", "", "Backend override: fake, kafka, rabbitmq, pulsar, rocketmq")
	cmd.PersistentFlags().StringVar(&f.Cluster, "cluster", "", "Broker cluster")
	cmd.PersistentFlags().StringVarP(&f.Namespace, "namespace", "n", "", "Broker namespace")
	cmd.PersistentFlags().DurationVar(&f.Timeout, "timeout", defaultCommandTimeout, "Request timeout")
	cmd.PersistentFlags().StringVarP(&f.Output, "output", "o", "table", "Output format: table | json | plain")
	cmd.PersistentFlags().BoolVar(&f.PlainHead, "plain-header", false, "Show headers in plain output")
	cmd.PersistentFlags().BoolVar(&f.Debug, "debug", false, "Enable debug logging")
	cmd.PersistentFlags().BoolVar(&f.Trace, "trace", false, "Enable trace logging (implies --debug)")
	cmd.PersistentFlags().BoolVar(&f.NoColor, "no-color", false, "Disable colored output")
	cmd.PersistentFlags().Int64Var(&f.AuditMaxSize, "audit-max-size", audit.DefaultMaxSizeBytes, "Active audit log rotation size in bytes")
	cmd.PersistentFlags().BoolVar(&f.DryRun, "dry-run", false, "Plan only, do not mutate")
	cmd.PersistentFlags().BoolVar(&f.Plan, "plan", false, "Alias for --dry-run plan output")
	cmd.PersistentFlags().BoolVar(&f.Yes, "yes", false, "Confirm write authorization")
	cmd.PersistentFlags().StringVar(&f.Ticket, "ticket", "", "Human-supplied change ticket")
	cmd.PersistentFlags().StringVar(&f.Operator, "operator", "", "Operator identity")
	cmd.PersistentFlags().StringVar(&f.Reason, "reason", "", "Change reason")
	cmd.PersistentFlags().BoolVar(&f.NonInter, "non-interactive", false, "Disable interactive confirmation")
	cmd.PersistentFlags().BoolVar(&f.AllowOffsetReset, "allow-offset-reset", false, "Allow R3 offset reset/seek")
	cmd.PersistentFlags().BoolVar(&f.AllowTopicPurge, "allow-topic-purge", false, "Allow R3 topic or DLQ purge")
	cmd.PersistentFlags().BoolVar(&f.AllowTopicDelete, "allow-topic-delete", false, "Allow R3 topic delete")
	cmd.PersistentFlags().BoolVar(&f.AllowDestructiveACL, "allow-destructive-acl", false, "Allow R3 destructive ACL operation")
	cmd.PersistentFlags().BoolVar(&f.AllowInternalProd, "allow-internal-produce", false, "Allow R3 produce to internal/system topics")
	cmd.PersistentFlags().BoolVar(&f.AllowSchemaDelete, "allow-schema-delete", false, "Allow R3 schema delete")
	cmd.PersistentFlags().StringVar(&f.OTLPEnd, "otel-endpoint", "", "OTLP trace endpoint")
	cmd.PersistentFlags().StringVar(&f.OTLPMetrics, "otel-metrics-endpoint", "", "OTLP metrics endpoint")
	cmd.PersistentFlags().BoolVar(&f.OTLPInsec, "otel-insecure", false, "Disable TLS for OTLP exporter")
	cmd.AddCommand(newTopicCmd(f), newGroupCmd(f), newMessageCmd(f), newDLQCmd(f), newACLCmd(f), newSchemaCmd(f), newFleetCmd(f), newContextCmd(f), newAuditCmd(f), newInstallCmd(f), newCapabilitiesCmd(f), newDoctorCmd(f), newVersionCmd(f))
	return cmd
}

func applyGlobalFlags(f *cliFlags) {
	if f.Trace {
		f.Debug = true
	}
	if f.NoColor {
		_ = os.Setenv("NO_COLOR", "1")
		color.NoColor = true
	}
}

func Execute() {
	if os.Getenv("NO_COLOR") != "" || !isatty.IsTerminal(os.Stdout.Fd()) {
		color.NoColor = true
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	f := newDefaultFlags()
	cmd := newRootCmdWith(f)
	err := cmd.ExecuteContext(ctx)
	finishTelemetry(ctx, f, err)
	if err == nil || errors.Is(err, context.Canceled) {
		stop()
		return
	}
	code := apperrors.ExitCode(err)
	if outputFlagFromArgs(os.Args[1:]) == "json" {
		_ = apperrors.WriteJSON(os.Stderr, err)
	} else {
		appErr := apperrors.AsAppError(err)
		_, _ = fmt.Fprintf(os.Stderr, "x %s\n", appErr.Error())
		if appErr.Suggestion != "" {
			_, _ = fmt.Fprintf(os.Stderr, "\nSuggestion:\n  %s\n", appErr.Suggestion)
		}
	}
	stop()
	os.Exit(code)
}

func finishTelemetry(ctx context.Context, f *cliFlags, err error) {
	if f.activeSpan != nil {
		if err != nil {
			f.activeSpan.RecordError(err)
			f.activeSpan.SetStatus(codes.Error, err.Error())
		} else {
			f.activeSpan.SetStatus(codes.Ok, "")
		}
		f.activeSpan.End()
	}
	if !f.commandTime.IsZero() {
		status := "success"
		if err != nil {
			status = "error"
		}
		telemetry.RecordCommand(ctx, f.commandName, status, time.Since(f.commandTime), f.metricAttrs)
	}
	if f.telemetryStop != nil || f.metricsStop != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if f.telemetryStop != nil {
			f.telemetryStop(shutdownCtx)
		}
		if f.metricsStop != nil {
			f.metricsStop(shutdownCtx)
		}
		cancel()
	}
}

func buildBroker(f *cliFlags) (mqgov.Broker, mqgovctx.Context, error) {
	item, name, err := resolvedContext(f)
	if err != nil {
		return nil, mqgovctx.Context{}, err
	}
	backendName := firstNonEmpty(f.Backend, item.Backend, defaultFakeBackend)
	if backendName != defaultFakeBackend {
		if backendName == "kafka" {
			backend, err := buildKafkaBackend(f, item, name)
			return backend, item, err
		}
		if backendName == "rabbitmq" {
			backend, err := buildRabbitMQBackend(f, item, name)
			return backend, item, err
		}
		if backendName == "pulsar" {
			backend, err := buildPulsarBackend(f, item, name)
			return backend, item, err
		}
		if backendName == "rocketmq" {
			backend, err := buildRocketMQBackend(f, item, name)
			return backend, item, err
		}
		return nil, mqgovctx.Context{}, apperrors.New(apperrors.CodeNotImplemented, "backend is not supported", nil)
	}
	cluster := firstNonEmpty(f.Cluster, item.Cluster, "fake")
	namespace := firstNonEmpty(f.Namespace, item.Namespace)
	return fake.New(cluster, namespace), item, nil
}

func buildPulsarBackend(f *cliFlags, item mqgovctx.Context, contextName string) (mqgov.Broker, error) {
	token, err := mqgovctx.ResolvePassword(context.Background(), contextName, item)
	if err != nil {
		return nil, err
	}
	return pulsarbackend.New(pulsarbackend.Options{
		ServiceURL:     firstNonEmpty(item.PulsarServiceURL, os.Getenv("PULSAR_SERVICE_URL")),
		AdminURL:       firstNonEmpty(item.PulsarAdminURL, os.Getenv("PULSAR_ADMIN_URL")),
		Tenant:         firstNonEmpty(item.PulsarTenant, os.Getenv("PULSAR_TENANT"), "public"),
		Namespace:      firstNonEmpty(item.PulsarNamespace, os.Getenv("PULSAR_NAMESPACE"), "default"),
		Cluster:        firstNonEmpty(f.Cluster, item.Cluster, "pulsar"),
		Token:          firstNonEmpty(os.Getenv("PULSAR_TOKEN"), token),
		TLS:            item.PulsarTLS || os.Getenv("PULSAR_TLS") == "true",
		CACertFile:     firstNonEmpty(item.PulsarCACertFile, os.Getenv("PULSAR_CA_CERT_FILE")),
		ClientCertFile: firstNonEmpty(item.PulsarClientCertFile, os.Getenv("PULSAR_CLIENT_CERT_FILE")),
		ClientKeyFile:  firstNonEmpty(item.PulsarClientKeyFile, os.Getenv("PULSAR_CLIENT_KEY_FILE")),
		Timeout:        f.Timeout,
	})
}

func buildRocketMQBackend(f *cliFlags, item mqgovctx.Context, contextName string) (mqgov.Broker, error) {
	secretKey, err := mqgovctx.ResolvePassword(context.Background(), contextName, item)
	if err != nil {
		return nil, err
	}
	return rocketmqbackend.New(rocketmqbackend.Options{
		NameServers:    firstNonEmptyList(item.RocketMQNameServers, os.Getenv("ROCKETMQ_NAMESRV_ADDR")),
		BrokerAddr:     firstNonEmpty(item.RocketMQBrokerAddr, os.Getenv("ROCKETMQ_BROKER_ADDR")),
		Cluster:        firstNonEmpty(f.Cluster, item.Cluster, "rocketmq"),
		Namespace:      firstNonEmpty(f.Namespace, item.Namespace),
		AccessKey:      firstNonEmpty(item.RocketMQAccessKey, os.Getenv("ROCKETMQ_ACCESS_KEY")),
		SecretKey:      firstNonEmpty(os.Getenv("ROCKETMQ_SECRET_KEY"), secretKey),
		TLS:            item.RocketMQTLS || os.Getenv("ROCKETMQ_TLS") == "true",
		CACertFile:     firstNonEmpty(item.RocketMQCACertFile, os.Getenv("ROCKETMQ_CA_CERT_FILE")),
		ClientCertFile: firstNonEmpty(item.RocketMQClientCertFile, os.Getenv("ROCKETMQ_CLIENT_CERT_FILE")),
		ClientKeyFile:  firstNonEmpty(item.RocketMQClientKeyFile, os.Getenv("ROCKETMQ_CLIENT_KEY_FILE")),
		Timeout:        f.Timeout,
	})
}

func buildRabbitMQBackend(f *cliFlags, item mqgovctx.Context, contextName string) (mqgov.Broker, error) {
	password, err := mqgovctx.ResolvePassword(context.Background(), contextName, item)
	if err != nil {
		return nil, err
	}
	return rabbitmqbackend.New(rabbitmqbackend.Options{
		AMQPURL:        firstNonEmpty(item.RabbitMQAMQPURL, os.Getenv("RABBITMQ_AMQP_URL")),
		ManagementURL:  firstNonEmpty(item.RabbitMQManagementURL, os.Getenv("RABBITMQ_MANAGEMENT_URL")),
		Host:           firstNonEmpty(item.RabbitMQHost, os.Getenv("RABBITMQ_HOST")),
		Port:           firstNonZeroInt(item.RabbitMQPort, envInt("RABBITMQ_PORT")),
		VHost:          firstNonEmpty(item.RabbitMQVHost, os.Getenv("RABBITMQ_VHOST")),
		Cluster:        firstNonEmpty(f.Cluster, item.Cluster, "rabbitmq"),
		Namespace:      firstNonEmpty(f.Namespace, item.Namespace),
		Username:       firstNonEmpty(item.Username, os.Getenv("RABBITMQ_USERNAME")),
		Password:       firstNonEmpty(os.Getenv("RABBITMQ_PASSWORD"), password),
		TLS:            item.RabbitMQTLS || os.Getenv("RABBITMQ_TLS") == "true",
		CACertFile:     firstNonEmpty(item.RabbitMQCACertFile, os.Getenv("RABBITMQ_CA_CERT_FILE")),
		ClientCertFile: firstNonEmpty(item.RabbitMQClientCertFile, os.Getenv("RABBITMQ_CLIENT_CERT_FILE")),
		ClientKeyFile:  firstNonEmpty(item.RabbitMQClientKeyFile, os.Getenv("RABBITMQ_CLIENT_KEY_FILE")),
		Timeout:        f.Timeout,
	})
}

func buildKafkaBackend(f *cliFlags, item mqgovctx.Context, contextName string) (mqgov.Broker, error) {
	password, err := mqgovctx.ResolvePassword(context.Background(), contextName, item)
	if err != nil {
		return nil, err
	}
	srPassword, err := mqgovctx.ResolveKafkaSchemaRegistryPassword(context.Background(), contextName, item)
	if err != nil {
		return nil, err
	}
	return kafkabackend.New(kafkabackend.Options{
		Brokers:                firstNonEmptyList(item.KafkaBrokers, os.Getenv("KAFKA_BROKERS")),
		Cluster:                firstNonEmpty(f.Cluster, item.Cluster, "kafka"),
		Namespace:              firstNonEmpty(f.Namespace, item.Namespace),
		Username:               firstNonEmpty(item.Username, os.Getenv("KAFKA_USERNAME")),
		Password:               firstNonEmpty(os.Getenv("KAFKA_PASSWORD"), password),
		SASLMechanism:          firstNonEmpty(item.KafkaSASLMechanism, os.Getenv("KAFKA_SASL_MECHANISM")),
		TLS:                    item.KafkaTLS || os.Getenv("KAFKA_TLS") == "true",
		CACertFile:             firstNonEmpty(item.KafkaCACertFile, os.Getenv("KAFKA_CA_CERT_FILE")),
		ClientCertFile:         firstNonEmpty(item.KafkaClientCertFile, os.Getenv("KAFKA_CLIENT_CERT_FILE")),
		ClientKeyFile:          firstNonEmpty(item.KafkaClientKeyFile, os.Getenv("KAFKA_CLIENT_KEY_FILE")),
		SchemaRegistryURL:      firstNonEmpty(item.KafkaSchemaRegistryURL, os.Getenv("KAFKA_SCHEMA_REGISTRY_URL")),
		SchemaRegistryUsername: firstNonEmpty(item.KafkaSchemaRegistryUsername, os.Getenv("KAFKA_SCHEMA_REGISTRY_USERNAME")),
		SchemaRegistryPassword: firstNonEmpty(os.Getenv("KAFKA_SCHEMA_REGISTRY_PASSWORD"), srPassword),
		Timeout:                f.Timeout,
	})
}

func resolvedContext(f *cliFlags) (mqgovctx.Context, string, error) {
	if f.Context != "" {
		cfg, err := mqgovctx.Load()
		if err != nil {
			return mqgovctx.Context{}, "", err
		}
		item, ok := cfg.Contexts[f.Context]
		if !ok {
			return mqgovctx.Context{}, "", apperrors.New(apperrors.CodeUsageError, "context not found", nil)
		}
		return item, f.Context, nil
	}
	ctx, name, err := mqgovctx.Current()
	if err == nil {
		return *ctx, name, nil
	}
	return mqgovctx.Context{Backend: firstNonEmpty(f.Backend, defaultFakeBackend), Cluster: f.Cluster, Namespace: f.Namespace}, "direct", nil
}

func authorize(f *cliFlags, base safety.Risk, meta mqgovctx.Context, required safety.AllowFlag) error {
	risk := safety.EffectiveRisk(base, safety.ContextMeta{
		Env:             meta.Env,
		Protected:       meta.Protected,
		TicketPattern:   meta.TicketPattern,
		TicketValidator: meta.TicketValidator,
		Roles:           meta.Roles,
	})
	err := safety.Authorize(risk, safety.Options{
		Yes:                f.Yes,
		NonInteractive:     f.NonInter,
		Ticket:             f.Ticket,
		TicketPattern:      meta.TicketPattern,
		Validator:          ticketValidator(meta.TicketValidator, f.contextName(), currentOperator(f)),
		RequiredAllowFlags: requiredAllow(required),
		GrantedAllowFlags: map[safety.AllowFlag]bool{
			allowOffsetReset:     f.AllowOffsetReset,
			allowTopicPurge:      f.AllowTopicPurge,
			allowTopicDelete:     f.AllowTopicDelete,
			allowDestructiveACL:  f.AllowDestructiveACL,
			allowInternalProduce: f.AllowInternalProd,
			allowSchemaDelete:    f.AllowSchemaDelete,
		},
		Roles:    meta.Roles,
		Operator: currentOperator(f),
	})
	if err != nil {
		appendAuditWarn(f, audit.EventAuthorizationDenied, meta, audit.EventTarget{ResourceType: "mq"}, audit.StatusDenied, "", err)
	}
	return err
}

func classifyAndAuthorize(f *cliFlags, meta mqgovctx.Context, op mqclass.Operation, target mqclass.Target, allow safety.AllowFlag) error {
	result := mqclass.Classify(op, target)
	return authorize(f, result.Risk, meta, allow)
}

func ticketValidator(path, contextName, operator string) safety.TicketValidator {
	if path == "" {
		return nil
	}
	return safety.NewExternalValidator(path, contextName, operator)
}

func requiredAllow(flag safety.AllowFlag) []safety.AllowFlag {
	if flag == "" {
		return nil
	}
	return []safety.AllowFlag{flag}
}

func appendAuditWarn(f *cliFlags, typ audit.EventType, ctx mqgovctx.Context, target audit.EventTarget, status, diff string, err error) {
	path, pathErr := audit.DefaultPath()
	if pathErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: audit path failed: %v\n", pathErr)
		return
	}
	evt := audit.Event{
		EventType: typ,
		Operator:  currentOperator(f),
		Context:   audit.EventContext{Name: f.contextName(), Env: ctx.Env, Protected: ctx.Protected},
		Ticket:    f.Ticket,
		Reason:    redact.String(f.Reason),
		Target:    target,
		Status:    status,
		Diff:      redact.String(diff),
	}
	if err != nil {
		appErr := apperrors.AsAppError(err)
		evt.Error = &audit.EventError{Code: string(appErr.Code), Message: appErr.Message}
	}
	if appendErr := audit.AppendWithOptions(path, evt, auditOptions(f)); appendErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: audit write failed: %v\n", appendErr)
	}
}

func auditOptions(f *cliFlags) audit.Options {
	maxSize := f.AuditMaxSize
	if maxSize <= 0 {
		maxSize = audit.DefaultMaxSizeBytes
	}
	return audit.Options{MaxSizeBytes: maxSize}
}

func newPrinter(f *cliFlags) *printer.Printer {
	p := printer.New(printer.FormatTable)
	switch f.Output {
	case "json":
		p = printer.New(printer.FormatJSON)
	case "plain":
		p = printer.New(printer.FormatPlain)
	}
	p.PlainHead = f.PlainHead
	return p
}

func (f *cliFlags) contextName() string {
	if f.Context != "" {
		return f.Context
	}
	f.contextOnce.Do(func() {
		_, name, err := mqgovctx.Current()
		if err != nil || name == "" {
			name = "direct"
		}
		f.cachedCtx = name
	})
	return f.cachedCtx
}

func currentOperator(f *cliFlags) string {
	if f.Operator != "" {
		return f.Operator
	}
	if env := os.Getenv("MQGOV_CLI_OPERATOR"); env != "" {
		return env
	}
	if u, err := osuser.Current(); err == nil && u != nil && u.Username != "" {
		if host, herr := os.Hostname(); herr == nil && host != "" {
			return u.Username + "@" + host
		}
		return u.Username
	}
	return "unknown"
}

func resolveTelemetryConfig(f *cliFlags) (traceEndpoint, metricsEndpoint string, insecure bool, ctxMeta mqgovctx.Context, ctxName string) {
	ctxMeta, ctxName, _ = resolvedContext(f)
	traceEndpoint = firstNonEmpty(f.OTLPEnd, ctxMeta.OTLPEndpoint, os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	metricsEndpoint = firstNonEmpty(f.OTLPMetrics, ctxMeta.OTLPMetricsEndpoint, os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"))
	insecure = f.OTLPInsec || ctxMeta.OTLPInsecure
	if ctxName == "" {
		ctxName = f.contextName()
	}
	return traceEndpoint, metricsEndpoint, insecure, ctxMeta, ctxName
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstNonEmptyList(values []string, fallback string) []string {
	if len(values) > 0 {
		return values
	}
	if fallback == "" {
		return nil
	}
	return strings.Split(fallback, ",")
}

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func envInt(name string) int {
	value := os.Getenv(name)
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func validateOutput(output string) error {
	switch output {
	case "", "table", "json", "plain":
		return nil
	default:
		return apperrors.New(apperrors.CodeUsageError, "output format must be table, json, or plain", nil)
	}
}

func outputFlagFromArgs(args []string) string {
	for i, arg := range args {
		if (arg == "-o" || arg == "--output") && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--output=") {
			return strings.TrimPrefix(arg, "--output=")
		}
		if strings.HasPrefix(arg, "-o=") {
			return strings.TrimPrefix(arg, "-o=")
		}
	}
	return ""
}

func isProtectedTopic(meta mqgovctx.Context, topic string, desc mqgov.TopicDescription) bool {
	if desc.Protected {
		return true
	}
	for _, protected := range meta.Topics {
		if protected == topic {
			return true
		}
	}
	return false
}
