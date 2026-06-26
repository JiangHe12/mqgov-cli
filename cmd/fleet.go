package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/audit"

	"github.com/JiangHe12/mqgov-cli/internal/backend/fake"
	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

type fleetFlags struct {
	all      bool
	contexts string
}

type fleetStatusItem struct {
	Context      string             `json:"context"`
	Status       string             `json:"status"`
	Error        string             `json:"error,omitempty"`
	Backend      string             `json:"backend,omitempty"`
	Cluster      string             `json:"cluster,omitempty"`
	Namespace    string             `json:"namespace,omitempty"`
	Capabilities mqgov.Capabilities `json:"capabilities,omitempty"`
}

type fleetTopicItem struct {
	Context   string                   `json:"context"`
	Status    string                   `json:"status"`
	Error     string                   `json:"error,omitempty"`
	Backend   string                   `json:"backend,omitempty"`
	Cluster   string                   `json:"cluster,omitempty"`
	Namespace string                   `json:"namespace,omitempty"`
	Topics    []mqgov.TopicDescription `json:"topics,omitempty"`
	Count     int                      `json:"count"`
}

type fleetContext struct {
	name string
	item mqgovctx.Context
}

func newFleetCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "fleet", Short: "Aggregate read-only views across contexts"}
	cmd.AddCommand(newFleetStatusCmd(f), newFleetTopicsCmd(f))
	return cmd
}

func newFleetStatusCmd(f *cliFlags) *cobra.Command {
	var flags fleetFlags
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show health and capabilities for selected contexts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			contexts, err := selectedFleetContexts(flags)
			if err != nil {
				return err
			}
			opTarget := fleetOperationTarget(contexts)
			results := make([]fleetStatusItem, 0, len(contexts))
			for _, item := range contexts {
				results = append(results, fleetStatusForContext(cmd, f, item))
			}
			appendFleetAudit(f, contexts, fleetStatusAudit(results))
			if f.Output == "json" {
				return targetJSONList(f, "FleetStatus", results, len(results), len(results), opTarget)
			}
			rows := make([][]string, 0, len(results))
			for _, item := range results {
				rows = append(rows, []string{item.Context, item.Status, item.Backend, item.Cluster, item.Namespace, strconv.FormatBool(item.Capabilities.SupportsACL), strconv.FormatBool(item.Capabilities.SupportsSchema), item.Error})
			}
			targetTable(f, []string{"CONTEXT", "STATUS", "BACKEND", "CLUSTER", "NAMESPACE", "ACL", "SCHEMA", "ERROR"}, rows, opTarget)
			return nil
		},
	}
	addFleetSelectionFlags(cmd, &flags)
	return cmd
}

func newFleetTopicsCmd(f *cliFlags) *cobra.Command {
	var flags fleetFlags
	var pattern string
	var limit int
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "List topics across selected contexts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			contexts, err := selectedFleetContexts(flags)
			if err != nil {
				return err
			}
			opTarget := fleetOperationTarget(contexts)
			results := make([]fleetTopicItem, 0, len(contexts))
			for _, item := range contexts {
				results = append(results, fleetTopicsForContext(cmd, f, item, pattern, limit))
			}
			appendFleetAudit(f, contexts, fleetTopicsAudit(results))
			if f.Output == "json" {
				return targetJSONList(f, "FleetTopics", results, len(results), len(results), opTarget)
			}
			rows := make([][]string, 0)
			for _, item := range results {
				if item.Status != audit.StatusSuccess {
					rows = append(rows, []string{item.Context, item.Status, item.Backend, "", "", item.Error})
					continue
				}
				for _, topic := range item.Topics {
					rows = append(rows, []string{item.Context, item.Status, item.Backend, topic.Coordinate.Topic, strconv.Itoa(topic.Partitions), ""})
				}
			}
			targetTable(f, []string{"CONTEXT", "STATUS", "BACKEND", "TOPIC", "PARTITIONS", "ERROR"}, rows, opTarget)
			return nil
		},
	}
	addFleetSelectionFlags(cmd, &flags)
	cmd.Flags().StringVar(&pattern, "pattern", "", "Exact topic name")
	cmd.Flags().IntVar(&limit, "limit", 0, "Maximum topics per context")
	return cmd
}

func addFleetSelectionFlags(cmd *cobra.Command, flags *fleetFlags) {
	cmd.Flags().BoolVar(&flags.all, "all", false, "Select all configured contexts")
	cmd.Flags().StringVar(&flags.contexts, "contexts", "", "Comma-separated context names")
}

func selectedFleetContexts(flags fleetFlags) ([]fleetContext, error) {
	if flags.all && strings.TrimSpace(flags.contexts) != "" {
		return nil, apperrors.New(apperrors.CodeUsageError, "--all and --contexts are mutually exclusive", nil)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		return nil, err
	}
	if flags.all {
		names := make([]string, 0, len(cfg.Contexts))
		for name := range cfg.Contexts {
			names = append(names, name)
		}
		sort.Strings(names)
		return fleetContextsByName(cfg.Contexts, names)
	}
	names := splitCSV(flags.contexts)
	if len(names) == 0 {
		return nil, apperrors.New(apperrors.CodeUsageError, "select contexts with --all or --contexts", nil)
	}
	return fleetContextsByName(cfg.Contexts, names)
}

func fleetContextsByName(items map[string]mqgovctx.Context, names []string) ([]fleetContext, error) {
	out := make([]fleetContext, 0, len(names))
	for _, name := range names {
		item, ok := items[name]
		if !ok {
			return nil, apperrors.New(apperrors.CodeUsageError, "context not found: "+name, nil)
		}
		out = append(out, fleetContext{name: name, item: item})
	}
	if len(out) == 0 {
		return nil, apperrors.New(apperrors.CodeUsageError, "no contexts selected", nil)
	}
	return out, nil
}

func fleetStatusForContext(cmd *cobra.Command, f *cliFlags, item fleetContext) fleetStatusItem {
	backend, err := buildBrokerForFleetContext(f, item)
	if err != nil {
		return fleetStatusItem{Context: item.name, Status: fleetErrorStatus(err), Error: appErrorMessage(err)}
	}
	if err := classifyAndAuthorize(f, item.item, mqclass.OperationClusterInfo, mqclass.Target{}, ""); err != nil {
		return fleetStatusItem{Context: item.name, Status: fleetErrorStatus(err), Error: appErrorMessage(err)}
	}
	desc := backend.Describe()
	caps := backend.Capabilities()
	if err := backend.Ping(cmd.Context()); err != nil {
		return fleetStatusItem{Context: item.name, Status: fleetErrorStatus(err), Error: appErrorMessage(err), Backend: desc.Backend, Cluster: desc.Cluster, Namespace: desc.Namespace, Capabilities: caps}
	}
	return fleetStatusItem{Context: item.name, Status: audit.StatusSuccess, Backend: desc.Backend, Cluster: desc.Cluster, Namespace: desc.Namespace, Capabilities: caps}
}

func fleetTopicsForContext(cmd *cobra.Command, f *cliFlags, item fleetContext, pattern string, limit int) fleetTopicItem {
	backend, err := buildBrokerForFleetContext(f, item)
	if err != nil {
		return fleetTopicItem{Context: item.name, Status: fleetErrorStatus(err), Error: appErrorMessage(err)}
	}
	desc := backend.Describe()
	if err := classifyAndAuthorize(f, item.item, mqclass.OperationList, mqclass.Target{Topic: pattern}, ""); err != nil {
		return fleetTopicItem{Context: item.name, Status: fleetErrorStatus(err), Error: appErrorMessage(err), Backend: desc.Backend, Cluster: desc.Cluster, Namespace: desc.Namespace}
	}
	topics, err := backend.ListTopics(cmd.Context(), mqgov.TopicListOptions{Pattern: pattern, Limit: limit})
	if err != nil {
		return fleetTopicItem{Context: item.name, Status: fleetErrorStatus(err), Error: appErrorMessage(err), Backend: desc.Backend, Cluster: desc.Cluster, Namespace: desc.Namespace}
	}
	return fleetTopicItem{Context: item.name, Status: audit.StatusSuccess, Backend: desc.Backend, Cluster: desc.Cluster, Namespace: desc.Namespace, Topics: topics, Count: len(topics)}
}

func buildBrokerForFleetContext(f *cliFlags, item fleetContext) (mqgov.Broker, error) {
	local := fleetLocalFlags(f, item.name)
	return buildBrokerFromContext(&local, item.item, item.name)
}

func fleetLocalFlags(f *cliFlags, contextName string) cliFlags {
	return cliFlags{
		Config:              f.Config,
		Context:             contextName,
		Timeout:             f.Timeout,
		Output:              f.Output,
		PlainHead:           f.PlainHead,
		AuditMaxSize:        f.AuditMaxSize,
		Yes:                 f.Yes,
		Ticket:              f.Ticket,
		Operator:            f.Operator,
		Reason:              f.Reason,
		NonInter:            f.NonInter,
		AllowOffsetReset:    f.AllowOffsetReset,
		AllowTopicPurge:     f.AllowTopicPurge,
		AllowTopicDelete:    f.AllowTopicDelete,
		AllowDestructiveACL: f.AllowDestructiveACL,
		AllowInternalProd:   f.AllowInternalProd,
		AllowSchemaDelete:   f.AllowSchemaDelete,
	}
}

func buildBrokerFromContext(f *cliFlags, item mqgovctx.Context, name string) (mqgov.Broker, error) {
	switch item.Backend {
	case "kafka":
		return buildKafkaBackend(f, item, name)
	case "rabbitmq":
		return buildRabbitMQBackend(f, item, name)
	case "pulsar":
		return buildPulsarBackend(f, item, name)
	case "rocketmq":
		return buildRocketMQBackend(f, item, name)
	case "", defaultFakeBackend:
		return fakeBroker(f, item), nil
	default:
		return nil, apperrors.New(apperrors.CodeNotImplemented, "backend is not supported", nil)
	}
}

func fakeBroker(f *cliFlags, item mqgovctx.Context) mqgov.Broker {
	return fake.New(firstNonEmpty(item.Cluster, f.Cluster, "fake"), firstNonEmpty(item.Namespace, f.Namespace))
}

func appendFleetAudit(f *cliFlags, contexts []fleetContext, diff string) {
	names := make([]string, 0, len(contexts))
	for _, item := range contexts {
		names = append(names, item.name)
	}
	appendAuditWarn(f, auditEventFleet, mqgovctx.Context{}, audit.EventTarget{ResourceType: "fleet", Resource: strings.Join(names, ",")}, audit.StatusSuccess, diff, nil)
}

func fleetStatusAudit(items []fleetStatusItem) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, item.Context+"="+item.Status)
	}
	return "status " + strings.Join(parts, ",")
}

func fleetTopicsAudit(items []fleetTopicItem) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%s=%s count=%d", item.Context, item.Status, item.Count))
	}
	return "topics " + strings.Join(parts, ",")
}

func appErrorMessage(err error) string {
	appErr := apperrors.AsAppError(err)
	return string(appErr.Code) + ": " + appErr.Message
}

func fleetErrorStatus(err error) string {
	code := apperrors.AsAppError(err).Code
	if code == apperrors.CodeAuthorizationRequired || code == apperrors.CodeAuthFailed {
		return "denied"
	}
	if code == apperrors.CodeBackendUnreachable {
		return "unreachable"
	}
	return "error"
}
