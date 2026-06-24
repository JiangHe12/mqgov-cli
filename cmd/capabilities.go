package cmd

import (
	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/credstore"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type capabilitiesData struct {
	Tool      capTool      `json:"tool"`
	Backend   capBackend   `json:"backend"`
	Supported capSupported `json:"supported"`
}

type capTool struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Commit  string `json:"commit"`
}

type capBackend struct {
	Name               string   `json:"name"`
	ResourceTypes      []string `json:"resourceTypes"`
	Verbs              []string `json:"verbs"`
	SupportsOffsets    bool     `json:"supportsOffsets"`
	SupportsPartitions bool     `json:"supportsPartitions"`
	SupportsACL        bool     `json:"supportsAcl"`
	SupportsDLQList    bool     `json:"supportsDlqList"`
	SupportsDLQPeek    bool     `json:"supportsDlqPeek"`
	SupportsDLQRedrive bool     `json:"supportsDlqRedrive"`
	SupportsDLQPurge   bool     `json:"supportsDlqPurge"`
	SupportsSchema     bool     `json:"supportsSchema"`
}

type capSupported struct {
	Commands           []capCommand `json:"commands"`
	ContextAPIVersions []string     `json:"contextApiVersions"`
	AuditAPIVersions   []string     `json:"auditApiVersions"`
	OutputFormats      []string     `json:"outputFormats"`
	ErrorCodes         []string     `json:"errorCodes"`
	ExitCodes          []int        `json:"exitCodes"`
	CredentialBackends []string     `json:"credentialBackends"`
}

type capCommand struct {
	Noun      string `json:"noun"`
	Verb      string `json:"verb"`
	Risk      string `json:"risk"`
	AllowFlag string `json:"allowFlag,omitempty"`
}

func newCapabilitiesCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "capabilities",
		Short: "Show mqgov capabilities",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			backend, _, err := buildBroker(f)
			if err != nil {
				return err
			}
			data := buildCapabilities(backend.Capabilities())
			if f.Output == "json" || f.Output == "plain" {
				return newPrinter(f).JSONData("Capabilities", data)
			}
			rows := make([][]string, 0, len(data.Supported.Commands))
			for _, cmd := range data.Supported.Commands {
				rows = append(rows, []string{cmd.Noun, cmd.Verb, cmd.Risk, cmd.AllowFlag})
			}
			newPrinter(f).Table([]string{"NOUN", "VERB", "RISK", "ALLOW FLAG"}, rows)
			return nil
		},
	}
}

func buildCapabilities(backendCaps mqgov.Capabilities) capabilitiesData {
	v, c, _ := getVersionInfo()
	return capabilitiesData{
		Tool: capTool{Name: "mqgov-cli", Version: v, Commit: c},
		Backend: capBackend{
			Name:               backendCaps.Backend,
			ResourceTypes:      backendCaps.ResourceTypes,
			Verbs:              backendCaps.Verbs,
			SupportsOffsets:    backendCaps.SupportsOffsets,
			SupportsPartitions: backendCaps.SupportsPartitions,
			SupportsACL:        backendCaps.SupportsACL,
			SupportsDLQList:    backendCaps.SupportsDLQList,
			SupportsDLQPeek:    backendCaps.SupportsDLQPeek,
			SupportsDLQRedrive: backendCaps.SupportsDLQRedrive,
			SupportsDLQPurge:   backendCaps.SupportsDLQPurge,
			SupportsSchema:     backendCaps.SupportsSchema,
		},
		Supported: capSupported{
			Commands: []capCommand{
				{Noun: "topic", Verb: "list/describe", Risk: "R0"},
				{Noun: "topic", Verb: "create", Risk: "R1/R2 protected"},
				{Noun: "topic", Verb: "alter", Risk: "R2"},
				{Noun: "topic", Verb: "purge", Risk: "R3", AllowFlag: "allow-topic-purge"},
				{Noun: "topic", Verb: "delete", Risk: "R3", AllowFlag: "allow-topic-delete"},
				{Noun: "group", Verb: "list/lag", Risk: "R0"},
				{Noun: "group", Verb: "create/delete", Risk: "R2"},
				{Noun: "group", Verb: "reset-offset", Risk: "R3", AllowFlag: "allow-offset-reset"},
				{Noun: "message", Verb: "peek", Risk: "R0"},
				{Noun: "message", Verb: "tail", Risk: "R0"},
				{Noun: "message", Verb: "produce", Risk: "R1/R2 protected"},
				{Noun: "message", Verb: "produce internal/system", Risk: "R3", AllowFlag: "allow-internal-produce"},
				{Noun: "dlq", Verb: "list/peek", Risk: "R0"},
				{Noun: "dlq", Verb: "redrive", Risk: "R3", AllowFlag: "allow-internal-produce"},
				{Noun: "dlq", Verb: "purge", Risk: "R3", AllowFlag: "allow-topic-purge"},
				{Noun: "acl", Verb: "list", Risk: "R0"},
				{Noun: "acl", Verb: "grant", Risk: "R2/R3 broad", AllowFlag: "allow-destructive-acl for R3"},
				{Noun: "acl", Verb: "revoke", Risk: "R3", AllowFlag: "allow-destructive-acl"},
				{Noun: "schema", Verb: "list/describe/check", Risk: "R0"},
				{Noun: "fleet", Verb: "status/topics", Risk: "R0"},
			},
			ContextAPIVersions: []string{"mqgov-cli.io/context/v1"},
			AuditAPIVersions:   []string{auditAPIVersion},
			OutputFormats:      []string{"table", "json", "plain"},
			ErrorCodes:         errorCodeStrings(),
			ExitCodes:          apperrors.AllExitCodes(),
			CredentialBackends: credstore.Available(),
		},
	}
}

func errorCodeStrings() []string {
	codes := apperrors.AllCodes()
	out := make([]string, 0, len(codes))
	for _, code := range codes {
		out = append(out, string(code))
	}
	return out
}
