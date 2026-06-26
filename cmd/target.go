package cmd

import (
	"strings"

	"github.com/JiangHe12/opskit-core/printer"

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

func printOperationTarget(p *printer.Printer, target operationTarget, mode operationTargetMode) {
	label := "TARGET"
	if mode == operationTargetWrite {
		label = "WRITE TARGET"
	}
	p.TargetHeader(label, [][2]string{
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
	printOperationTarget(p, target, mode)
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

func targetTable(f *cliFlags, headers []string, rows [][]string, target operationTarget) {
	p := newPrinter(f)
	printOperationTarget(p, target, operationTargetRead)
	p.Table(headers, rows)
}
