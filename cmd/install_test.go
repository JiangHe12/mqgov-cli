package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallSkillsCopiesSkillAndManifest(t *testing.T) {
	srcRoot := t.TempDir()
	srcDir := filepath.Join(srcRoot, "skills", "mqgov-cli")
	if err := os.MkdirAll(srcDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "SKILL.md"), []byte("---\nname: mqgov-cli\n---\n# mqgov-cli\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldFS := skillFS
	skillFS = os.DirFS(srcRoot)
	t.Cleanup(func() { skillFS = oldFS })

	target := t.TempDir()
	if err := installSkills(newDefaultFlags(), target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(target, "mqgov-cli", "SKILL.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(target, "mqgov-cli", ".installed-by")); err != nil {
		t.Fatal(err)
	}
}

func TestSkillFrontmatter(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "skills", "mqgov-cli", "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{"---", "name: mqgov-cli", "description:", "allowed-tools:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("SKILL.md missing %q", want)
		}
	}
}
