package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newVersionCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Display version information",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			v, c, b := getVersionInfo()
			data := map[string]string{"version": v, "commit": c, "built": b}
			if f.Output == "json" {
				return newPrinter(f).JSONData("VersionInfo", data)
			}
			_, _ = fmt.Fprintf(newPrinter(f).Out, "mqgov-cli %s (commit: %s, built: %s)\n", v, c, b)
			return nil
		},
	}
}
