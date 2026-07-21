package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
)

func requireSubcommand(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "unknown command %q for %q", args[0], cmd.CommandPath())
	if suggestions := cmd.SuggestionsFor(args[0]); len(suggestions) > 0 {
		sb.WriteString("\n\nDid you mean this?\n")
		for _, suggestion := range suggestions {
			_, _ = fmt.Fprintf(&sb, "\t%s\n", suggestion)
		}
	}
	return apperrors.New(apperrors.CodeUsageError, sb.String(), nil)
}

func runParentHelp(cmd *cobra.Command, _ []string) error {
	names := make([]string, 0, len(cmd.Commands()))
	for _, child := range cmd.Commands() {
		if child.Hidden {
			continue
		}
		names = append(names, child.Name())
	}
	return apperrors.New(
		apperrors.CodeUsageError,
		fmt.Sprintf("%s requires a subcommand", cmd.CommandPath()),
		nil,
	).WithSuggestion("Available subcommands: " + strings.Join(names, ", "))
}
