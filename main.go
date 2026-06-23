package main

import "github.com/JiangHe12/mqgov-cli/cmd"

func main() {
	cmd.SetSkillFS(skillEmbedFS)
	cmd.Execute()
}
