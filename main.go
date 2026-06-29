package main

import "github.com/JiangHe12/mqgov-cli/cmd"

var (
	version = "dev"
	commit  = "unknown"
	built   = "unknown"
)

func main() {
	cmd.SetVersionInfo(version, commit, built)
	cmd.SetSkillFS(skillEmbedFS)
	cmd.Execute()
}
