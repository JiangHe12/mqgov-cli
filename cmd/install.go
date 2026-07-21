package cmd

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
)

var agentPaths = map[string]string{
	"claude":    ".claude/skills",
	"codex":     ".codex/skills",
	"opencode":  ".opencode/skills",
	"copilot":   ".copilot/skills",
	"cursor":    ".cursor/skills",
	"cc-switch": ".cc-switch/skills",
	"windsurf":  ".windsurf/skills",
	"aider":     ".aider/skills",
}

var skillFS fs.FS

func SetSkillFS(fsys fs.FS) {
	skillFS = fsys
}

func newInstallCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "install <agent>",
		Short: "Install mqgov AI skill to an agent skills directory",
		Long: `Install mqgov-cli skill to the specified AI agent's skills directory.

Preset agents:
  claude      -> ~/.claude/skills/
  codex       -> ~/.codex/skills/
  opencode    -> ~/.opencode/skills/
  copilot     -> ~/.copilot/skills/
  cursor      -> ~/.cursor/skills/
  cc-switch   -> ~/.cc-switch/skills/
  windsurf    -> ~/.windsurf/skills/
  aider       -> ~/.aider/skills/

Custom path:
  mqgov install /my/path --skills  -> /my/path/mqgov-cli/`,
		Example: `  mqgov install claude --skills
  mqgov install codex --skills
  mqgov install /custom/path --skills`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return apperrors.New(apperrors.CodeUsageError, "install requires exactly one agent or path", nil)
			}
			if !cmd.Flags().Changed("skills") {
				return apperrors.New(apperrors.CodeUsageError, "please specify --skills flag", nil)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			skills, _ := cmd.Flags().GetBool("skills")
			if !skills {
				return apperrors.New(apperrors.CodeUsageError, "please specify --skills flag", nil)
			}
			return installSkills(f, args[0])
		},
	}
	cmd.Flags().Bool("skills", false, "Install skill files")
	_ = cmd.MarkFlagRequired("skills")
	return cmd
}

func installSkills(f *cliFlags, target string) error {
	installDir, err := resolveInstallDir(target)
	if err != nil {
		return err
	}
	dstDir := filepath.Join(installDir, "mqgov-cli")
	overwriting := skillInstallExists(dstDir)
	if contextPlanOnly(f) {
		return newPrinter(f).JSONData("ChangePlan", map[string]any{
			"resourceType": "file",
			"action":       "install skill",
			"path":         dstDir,
			"overwrite":    overwriting,
			"dryRun":       true,
		})
	}
	metadata := mutationValueMetadata("mq.install.skill", map[string]any{
		"path":      dstDir,
		"overwrite": overwriting,
	})
	metadata.Items = 1
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:   "mq.install.skill",
		Target:   audit.EventTarget{ResourceType: "file"},
		Metadata: metadata,
	})
	if err != nil {
		return err
	}
	operationErr := copyEmbeddedSkill(skillFS, "skills/mqgov-cli", dstDir)
	if operationErr == nil {
		operationErr = verifyInstalledSkill(dstDir)
	}
	if operationErr == nil {
		operationErr = writeInstallManifest(dstDir)
	}
	if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
		return err
	}
	p := newPrinter(f)
	if f.Output == "json" {
		return p.JSONData("InstallResult", map[string]string{"path": dstDir})
	}
	if overwriting {
		return p.Info(fmt.Sprintf("overwriting existing skill at %s", dstDir))
	}
	return p.Success(fmt.Sprintf("skill installed to %s", dstDir))
}

func writeInstallManifest(dstDir string) error {
	version, commit, _ := getVersionInfo()
	manifest := fmt.Sprintf("installed-by: mqgov-cli %s (commit: %s)\ninstalled-at: %s\n", version, commit, time.Now().UTC().Format(time.RFC3339))
	if err := os.WriteFile(filepath.Join(dstDir, ".installed-by"), []byte(manifest), 0o600); err != nil { //nolint:gosec // dstDir is user-selected install destination.
		return apperrors.New(apperrors.CodeLocalIOError, "failed to write install manifest", err)
	}
	return nil
}

func skillInstallExists(dstDir string) bool {
	_, err := os.Stat(filepath.Join(dstDir, "SKILL.md"))
	return err == nil
}

func verifyInstalledSkill(dstDir string) error {
	skillPath := filepath.Join(dstDir, "SKILL.md")
	info, err := os.Stat(skillPath)
	if err != nil || info.Size() == 0 {
		return apperrors.New(apperrors.CodeLocalIOError, fmt.Sprintf("installation appears to have failed: SKILL.md not present at %s after copy", skillPath), err)
	}
	return nil
}

func resolveInstallDir(target string) (string, error) {
	if skillsDir, ok := agentPaths[strings.ToLower(target)]; ok {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", apperrors.New(apperrors.CodeLocalIOError, "failed to get home directory", err)
		}
		return filepath.Join(home, skillsDir), nil
	}
	return target, nil
}

func copyEmbeddedSkill(fsys fs.FS, srcDir, dstDir string) error {
	if fsys == nil {
		return apperrors.New(apperrors.CodeLocalIOError, "embedded skill filesystem is not initialized", nil)
	}
	if err := os.MkdirAll(dstDir, 0o750); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to create skill directory", err)
	}
	entries, err := fs.ReadDir(fsys, srcDir)
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to read embedded skill", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		srcPath := path.Join(srcDir, entry.Name())
		dstPath := filepath.Join(dstDir, entry.Name())
		if entry.IsDir() {
			if err := copyEmbeddedSkill(fsys, srcPath, dstPath); err != nil {
				return err
			}
			continue
		}
		data, err := fs.ReadFile(fsys, srcPath)
		if err != nil {
			return apperrors.New(apperrors.CodeLocalIOError, "failed to read embedded skill file", err)
		}
		if err := os.WriteFile(dstPath, data, 0o600); err != nil { //nolint:gosec // dstPath is user-selected install destination.
			return apperrors.New(apperrors.CodeLocalIOError, "failed to write skill file", err)
		}
	}
	return nil
}
