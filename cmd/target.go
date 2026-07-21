package cmd

import (
	"strings"

	"github.com/JiangHe12/opskit-core/v2/printer"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type operationTargetMode string

const (
	operationTargetRead  operationTargetMode = "read"
	operationTargetWrite operationTargetMode = "write"
)

type operationTarget struct {
	Context   string `json:"context"`
	Backend   string `json:"backend"`
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
}

func operationTargetFromBroker(f *cliFlags, backend mqgov.Broker) operationTarget {
	return operationTargetFromDescription(f.contextName(), backend.Describe())
}

func operationTargetFromDescription(contextName string, desc mqgov.Description) operationTarget {
	return operationTarget{
		Context:   contextName,
		Backend:   desc.Backend,
		Cluster:   desc.Cluster,
		Namespace: desc.Namespace,
	}
}

func fleetOperationTarget(contexts []fleetContext) operationTarget {
	names := make([]string, 0, len(contexts))
	for _, item := range contexts {
		names = append(names, item.name)
	}
	return operationTarget{
		Context:   strings.Join(names, ","),
		Backend:   "fleet",
		Cluster:   "multiple",
		Namespace: "multiple",
	}
}

func printOperationTarget(p *printer.Printer, target operationTarget, mode operationTargetMode) error {
	label := "TARGET"
	if mode == operationTargetWrite {
		label = "WRITE TARGET"
	}
	return p.TargetHeader(label, [][2]string{
		{"context", target.Context},
		{"backend", target.Backend},
		{"cluster", target.Cluster},
		{"namespace", target.Namespace},
	})
}

func targetDataForOutput(f *cliFlags, data any, target operationTarget) any {
	if f.Output == "json" {
		return printer.WithTarget(data, target)
	}
	return data
}

func targetJSONData(f *cliFlags, kind string, data any, target operationTarget, mode operationTargetMode) error {
	p := newPrinter(f)
	if err := printOperationTarget(p, target, mode); err != nil {
		return err
	}
	return p.JSONData(kind, targetDataForOutput(f, data, target))
}

func targetJSONList(f *cliFlags, kind string, items any, total, pageSize int, target operationTarget) error {
	return newPrinter(f).JSONListEnvelope(printer.JSONListEnvelope{
		Kind:      kind,
		Items:     items,
		Total:     total,
		Page:      1,
		PageSize:  pageSize,
		Truncated: false,
		Target:    target,
	})
}

func printBrokerChangePlan(f *cliFlags, action, resourceType, resource string, details map[string]any) error {
	data := map[string]any{
		"action":       action,
		"resourceType": resourceType,
		"resource":     resource,
		"context":      f.contextName(),
		"dryRun":       true,
	}
	for key, value := range details {
		data[key] = value
	}
	return newPrinter(f).JSONData("ChangePlan", data)
}

func targetTable(f *cliFlags, headers []string, rows [][]string, target operationTarget) error {
	p := newPrinter(f)
	if err := printOperationTarget(p, target, operationTargetRead); err != nil {
		return err
	}
	return p.Table(headers, rows)
}
