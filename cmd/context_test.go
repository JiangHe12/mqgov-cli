package cmd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/credstore"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

type migrationTestBackend struct {
	values                   map[string]string
	availableCalls           int
	failAvailableCall        int
	putCalls                 int
	failPutCall              int
	failPutFromCall          int
	commitThenFailPutCall    int
	deleteCalls              int
	deleteContextErr         error
	deleteContextHasDeadline bool
}

func (backend *migrationTestBackend) Name() string { return "migration-test" }

func (backend *migrationTestBackend) Available() error {
	backend.availableCalls++
	if backend.availableCalls == backend.failAvailableCall {
		return errors.New("injected backend unavailable")
	}
	return nil
}

func (backend *migrationTestBackend) Get(_ context.Context, key string) (string, error) {
	value, ok := backend.values[key]
	if !ok {
		return "", credstore.ErrNotFound
	}
	return value, nil
}

func (backend *migrationTestBackend) Put(_ context.Context, key, value string) error {
	backend.putCalls++
	if backend.putCalls == backend.commitThenFailPutCall {
		backend.values[key] = value
		return errors.New("injected post-commit credential put failure")
	}
	if backend.putCalls == backend.failPutCall ||
		(backend.failPutFromCall > 0 && backend.putCalls >= backend.failPutFromCall) {
		return errors.New("injected credential put failure")
	}
	backend.values[key] = value
	return nil
}

func (backend *migrationTestBackend) Delete(ctx context.Context, key string) error {
	backend.deleteCalls++
	backend.deleteContextErr = ctx.Err()
	_, backend.deleteContextHasDeadline = ctx.Deadline()
	if err := ctx.Err(); err != nil {
		return err
	}
	delete(backend.values, key)
	return nil
}

func TestCtxSetRejectsPlainCredential(t *testing.T) {
	mqgovctx.SetConfigPath(filepath.Join(t.TempDir(), "config.yaml"))
	_, err := runCommandForTest(t,
		"-o", "json",
		"--backend", "rocketmq",
		"ctx", "set", "dev",
		"--nameservers", "127.0.0.1:9876",
		"--access-key", "ak",
		"--password", "secret-key",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeUsageError {
		t.Fatalf("error code = %s, want %s; err=%v", got, apperrors.CodeUsageError, err)
	}
}

func TestCtxSetStoresCredentialReference(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Setenv("MQGOV_CREDENTIAL_PASSPHRASE", "test-passphrase")

	out, err := runCommandForTest(t,
		"-o", "json",
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"--backend", "rocketmq",
		"ctx", "set", "dev",
		"--nameservers", "127.0.0.1:9876",
		"--broker-addr", "127.0.0.1:10911",
		"--access-key", "ak",
		"--password", "secret-key",
		"--credential-backend", "encrypted-file",
	)
	if err != nil {
		t.Fatalf("ctx set error = %v; out=%s", err, out)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-key") {
		t.Fatalf("context file leaked credential: %s", data)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	ref := credstore.ParseRef(cfg.Contexts["dev"].Password)
	if !ref.IsRef || ref.BackendName != "encrypted-file" {
		t.Fatalf("password ref = %#v; raw=%q", ref, cfg.Contexts["dev"].Password)
	}
}

func TestCtxSetStoresKafkaSchemaRegistryCredentialReference(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Setenv("MQGOV_CREDENTIAL_PASSPHRASE", "test-passphrase")

	out, err := runCommandForTest(t,
		"-o", "json",
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"--backend", "kafka",
		"ctx", "set", "dev",
		"--brokers", "127.0.0.1:9092",
		"--username", "kafka-user",
		"--schema-registry-url", "https://schema-registry.example",
		"--schema-registry-username", "sr-user",
		"--schema-registry-password", "sr-secret",
		"--credential-backend", "encrypted-file",
	)
	if err != nil {
		t.Fatalf("ctx set error = %v; out=%s", err, out)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "sr-secret") {
		t.Fatalf("context file leaked schema registry credential: %s", data)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	item := cfg.Contexts["dev"]
	if item.Username != "kafka-user" {
		t.Fatalf("Kafka username = %q, want kafka-user", item.Username)
	}
	if item.Password != "" {
		t.Fatalf("broker password ref = %q, want empty when only schema registry password was provided", item.Password)
	}
	if item.KafkaSchemaRegistryURL != "https://schema-registry.example" || item.KafkaSchemaRegistryUsername != "sr-user" {
		t.Fatalf("schema registry context fields = %+v", item)
	}
	ref := credstore.ParseRef(item.KafkaSchemaRegistryPassword)
	if !ref.IsRef || ref.BackendName != "encrypted-file" {
		t.Fatalf("schema registry password ref = %#v; raw=%q", ref, item.KafkaSchemaRegistryPassword)
	}
	resolved, err := mqgovctx.ResolveKafkaSchemaRegistryPassword(t.Context(), "dev", item)
	if err != nil {
		t.Fatalf("ResolveKafkaSchemaRegistryPassword() error = %v", err)
	}
	if resolved != "sr-secret" {
		t.Fatalf("resolved schema registry password = %q, want sr-secret", resolved)
	}
}

func TestCtxSetCompensatesWhenSchemaCredentialWriteFails(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	backend := &migrationTestBackend{
		values: map[string]string{
			"dev":                 "previous-primary",
			"dev/schema-registry": "previous-schema",
		},
		failPutCall: 2,
	}
	previousFactory := contextImportCredentialBackend
	contextImportCredentialBackend = func(mqgovctx.Context) (credstore.Backend, error) {
		return backend, nil
	}
	t.Cleanup(func() { contextImportCredentialBackend = previousFactory })

	_, err := runCommandForTestAtHome(t, home,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-context-change",
		"--backend", "kafka",
		"ctx", "set", "dev",
		"--brokers", "127.0.0.1:9092",
		"--password", "new-primary",
		"--schema-registry-password", "new-schema",
		"--credential-backend", credentialBackendEncrypted,
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeCredentialStoreError {
		t.Fatalf("ctx set code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, err)
	}
	assertFailedContextSetCompensated(t, home, backend, configPath, map[string]string{
		"dev":                 "previous-primary",
		"dev/schema-registry": "previous-schema",
	})
	if backend.putCalls != 3 {
		t.Fatalf("credential put calls = %d, want two attempts and one required reverse restore", backend.putCalls)
	}
}

func TestCtxSetCompensatesCredentialPutThatCommittedThenErrored(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	backend := &migrationTestBackend{
		values:                map[string]string{"dev": "previous-primary"},
		commitThenFailPutCall: 1,
	}
	previousFactory := contextImportCredentialBackend
	contextImportCredentialBackend = func(mqgovctx.Context) (credstore.Backend, error) {
		return backend, nil
	}
	t.Cleanup(func() { contextImportCredentialBackend = previousFactory })

	_, err := runCommandForTestAtHome(t, home,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-context-change",
		"--backend", "kafka",
		"ctx", "set", "dev",
		"--brokers", "127.0.0.1:9092",
		"--password", "new-primary",
		"--credential-backend", credentialBackendEncrypted,
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeCredentialStoreError {
		t.Fatalf("ctx set code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, err)
	}
	assertFailedContextSetCompensated(t, home, backend, configPath, map[string]string{
		"dev": "previous-primary",
	})
	if backend.putCalls != 2 {
		t.Fatalf("credential put calls = %d, want failed committed write and reverse restore", backend.putCalls)
	}
}

func TestCtxSetCompensatesWhenConfigCommitFails(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	backend := &migrationTestBackend{values: map[string]string{"dev": "previous-primary"}}
	previousFactory := contextImportCredentialBackend
	contextImportCredentialBackend = func(mqgovctx.Context) (credstore.Backend, error) {
		return backend, nil
	}
	t.Cleanup(func() { contextImportCredentialBackend = previousFactory })
	previousUpdate := contextSetUpdate
	contextSetUpdate = func(update func(*corectx.Config[mqgovctx.Context]) error) error {
		return mqgovctx.Update(func(cfg *corectx.Config[mqgovctx.Context]) error {
			if err := update(cfg); err != nil {
				return err
			}
			return apperrors.New(apperrors.CodeLocalIOError, "injected context commit failure", nil)
		})
	}
	t.Cleanup(func() { contextSetUpdate = previousUpdate })

	_, err := runCommandForTestAtHome(t, home,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-context-change",
		"--backend", "kafka",
		"ctx", "set", "dev",
		"--brokers", "127.0.0.1:9092",
		"--password", "new-primary",
		"--schema-registry-password", "new-schema",
		"--credential-backend", credentialBackendEncrypted,
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("ctx set code = %s, want %s; err=%v", got, apperrors.CodeLocalIOError, err)
	}
	assertFailedContextSetCompensated(t, home, backend, configPath, map[string]string{
		"dev": "previous-primary",
	})
	if backend.putCalls != 3 {
		t.Fatalf("credential put calls = %d, want two writes and one primary restore", backend.putCalls)
	}
	if !backend.deleteContextHasDeadline || backend.deleteContextErr != nil {
		t.Fatalf("compensation context deadline=%t error=%v", backend.deleteContextHasDeadline, backend.deleteContextErr)
	}
}

func TestCtxSetRefusesCompensationAfterCredentialTakeover(t *testing.T) {
	tests := []struct {
		name          string
		contextName   string
		credentialKey string
		args          []string
		ownerTakeover bool
	}{
		{
			name:          "value takeover",
			contextName:   "dev",
			credentialKey: "dev",
			args:          []string{"--password", "new-primary"},
		},
		{
			name:          "owner takeover",
			contextName:   "foo",
			credentialKey: "foo/schema-registry",
			args:          []string{"--schema-registry-password", "new-schema"},
			ownerTakeover: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			configPath := filepath.Join(home, "config.yaml")
			mqgovctx.SetConfigPath(configPath)
			t.Cleanup(func() { mqgovctx.SetConfigPath("") })
			backend := &migrationTestBackend{values: map[string]string{}}
			previousFactory := contextImportCredentialBackend
			contextImportCredentialBackend = func(mqgovctx.Context) (credstore.Backend, error) {
				return backend, nil
			}
			t.Cleanup(func() { contextImportCredentialBackend = previousFactory })

			injectedCommitErr := errors.New("injected context commit failure")
			previousUpdate := contextSetUpdate
			contextSetUpdate = func(update func(*corectx.Config[mqgovctx.Context]) error) error {
				operationErr := mqgovctx.Update(func(cfg *corectx.Config[mqgovctx.Context]) error {
					if err := update(cfg); err != nil {
						return err
					}
					return apperrors.New(apperrors.CodeLocalIOError, "injected context commit failure", injectedCommitErr)
				})
				if operationErr == nil {
					return errors.New("injected context commit failure was not returned")
				}
				interleaveErr := mqgovctx.Update(func(cfg *corectx.Config[mqgovctx.Context]) error {
					if tt.ownerTakeover {
						cfg.Contexts[tt.credentialKey] = mqgovctx.Context{
							Base:    corectx.Base{Password: credstore.EncodeRef(credentialBackendEncrypted)},
							Backend: "kafka",
						}
					}
					backend.values[tt.credentialKey] = "takeover-value"
					return nil
				})
				return errors.Join(operationErr, interleaveErr)
			}
			t.Cleanup(func() { contextSetUpdate = previousUpdate })

			args := []string{
				"--config", configPath,
				"--yes", "--ticket", "OPS-1", "--allow-context-change",
				"--backend", "kafka",
				"ctx", "set", tt.contextName,
				"--brokers", "127.0.0.1:9092",
			}
			args = append(args, tt.args...)
			args = append(args, "--credential-backend", credentialBackendEncrypted)
			_, err := runCommandForTestAtHome(t, home, args...)
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeCredentialStoreError {
				t.Fatalf("ctx set code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, err)
			}
			if !errors.Is(err, injectedCommitErr) {
				t.Fatalf("ctx set error lost original commit cause: %v", err)
			}
			if got := backend.values[tt.credentialKey]; got != "takeover-value" {
				t.Fatalf("credential takeover value = %q, want preserved takeover-value", got)
			}
			cfg, loadErr := mqgovctx.Load()
			if loadErr != nil {
				t.Fatalf("Load() error = %v", loadErr)
			}
			if _, exists := cfg.Contexts[tt.contextName]; exists {
				t.Fatalf("failed ctx set persisted target context %q", tt.contextName)
			}
			if tt.ownerTakeover {
				if _, exists := cfg.Contexts[tt.credentialKey]; !exists {
					t.Fatalf("owner takeover context %q was lost", tt.credentialKey)
				}
			}

			records := readSafeAuditRecords(t, filepath.Join(home, ".mqgov-cli", "audit.log"))
			if len(records) < 2 {
				t.Fatalf("audit records = %d, want intent and outcome", len(records))
			}
			outcome := records[len(records)-1].Outcome
			if records[len(records)-1].Status != audit.StatusFailed ||
				outcome == nil ||
				outcome.Succeeded != 0 ||
				outcome.Failed != 0 ||
				outcome.Uncertain != 1 ||
				outcome.CompensationStatus != credentialCompensationNotSafe {
				t.Fatalf("takeover audit outcome = %+v", outcome)
			}
		})
	}
}

func TestCtxSetReconcilesPostCommitErrorWithoutCredentialWrites(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	previousUpdate := contextSetUpdate
	contextSetUpdate = func(update func(*corectx.Config[mqgovctx.Context]) error) error {
		if err := mqgovctx.Update(update); err != nil {
			return err
		}
		return apperrors.New(apperrors.CodeLocalIOError, "injected post-commit context error", nil)
	}
	t.Cleanup(func() { contextSetUpdate = previousUpdate })

	_, err := runCommandForTestAtHome(t, home,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-context-change",
		"--backend", "kafka",
		"ctx", "set", "dev",
		"--brokers", "127.0.0.1:9092",
	)
	cfg, loadErr := mqgovctx.Load()
	if loadErr != nil {
		t.Fatalf("Load() error = %v", loadErr)
	}
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("ctx set code = %s, want %s; err=%v; contexts=%+v", got, apperrors.CodeLocalIOError, err, cfg.Contexts)
	}
	if _, exists := cfg.Contexts["dev"]; !exists {
		t.Fatal("post-commit error lost the committed context")
	}
	records := readSafeAuditRecords(t, filepath.Join(home, ".mqgov-cli", "audit.log"))
	if len(records) != 2 ||
		records[1].Status != audit.StatusPartialFailed ||
		records[1].Outcome == nil ||
		records[1].Outcome.Succeeded != 1 ||
		records[1].Outcome.Failed != 0 ||
		records[1].Outcome.Uncertain != 0 ||
		records[1].Outcome.CompensationStatus != "not-safe" {
		t.Fatalf("post-commit audit records = %+v", records)
	}
}

func TestCtxSetRejectsCredentialSlotCollisionIntroducedUnderLock(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	backend := &migrationTestBackend{values: map[string]string{}}
	previousFactory := contextImportCredentialBackend
	contextImportCredentialBackend = func(mqgovctx.Context) (credstore.Backend, error) {
		return backend, nil
	}
	t.Cleanup(func() { contextImportCredentialBackend = previousFactory })
	previousUpdate := contextSetUpdate
	contextSetUpdate = func(update func(*corectx.Config[mqgovctx.Context]) error) error {
		return mqgovctx.Update(func(cfg *corectx.Config[mqgovctx.Context]) error {
			cfg.Contexts["foo/schema-registry"] = mqgovctx.Context{
				Base:    corectx.Base{Password: credstore.EncodeRef(credentialBackendEncrypted)},
				Backend: "kafka",
			}
			return update(cfg)
		})
	}
	t.Cleanup(func() { contextSetUpdate = previousUpdate })

	_, err := runCommandForTestAtHome(t, home,
		"--config", configPath,
		"--yes", "--ticket", "OPS-1", "--allow-context-change",
		"--backend", "kafka",
		"ctx", "set", "foo",
		"--brokers", "127.0.0.1:9092",
		"--schema-registry-password", "new-schema",
		"--credential-backend", credentialBackendEncrypted,
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
		t.Fatalf("ctx set collision code = %s, want %s; err=%v", got, apperrors.CodeValidationFailed, err)
	}
	if backend.putCalls != 0 || len(backend.values) != 0 {
		t.Fatalf("collision wrote credentials: calls=%d values=%v", backend.putCalls, backend.values)
	}
}

func TestCtxImportRejectsCredentialSlotCollisionWithExistingContext(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("foo", mqgovctx.Context{
		Base: corectx.Base{
			CredentialBackend: credentialBackendEncrypted,
		},
		Backend:                     "kafka",
		KafkaSchemaRegistryPassword: credstore.EncodeRef(credentialBackendEncrypted),
	}); err != nil {
		t.Fatal(err)
	}
	backend := &migrationTestBackend{values: map[string]string{}}
	previousFactory := contextImportCredentialBackend
	contextImportCredentialBackend = func(mqgovctx.Context) (credstore.Backend, error) {
		return backend, nil
	}
	t.Cleanup(func() { contextImportCredentialBackend = previousFactory })
	flags := newDefaultFlags()
	flags.Yes = true
	flags.NonInter = true
	flags.Ticket = "OPS-1"
	flags.AllowContextChange = true
	flags.commandCtx = t.Context()
	doc := contextExportDocument{
		APIVersion: ctxExportAPIVersion,
		Name:       "foo/schema-registry",
		Context: &mqgovctx.Context{
			Base: corectx.Base{
				Password:          "new-primary",
				CredentialBackend: credentialBackendEncrypted,
			},
			Backend: "kafka",
		},
	}

	err := runCtxImportOne(flags, doc, ctxImportOptions{})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
		t.Fatalf("ctx import collision code = %s, want %s; err=%v", got, apperrors.CodeValidationFailed, err)
	}
	if backend.putCalls != 0 || len(backend.values) != 0 {
		t.Fatalf("collision import wrote credentials: calls=%d values=%v", backend.putCalls, backend.values)
	}
}

func TestVaultCredentialPhysicalSlotNormalization(t *testing.T) {
	primary := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
		Base: corectx.Base{
			VaultAddr:      "  https://vault.example///  ",
			VaultPath:      "  ///service/prod///  ",
			VaultNamespace: "team-a",
		},
	}, "vault", "alpha")
	schema := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
		Base: corectx.Base{
			VaultAddr:      "https://vault.example",
			VaultPath:      "service/prod",
			VaultNamespace: "team-a",
		},
	}, "vault", "bravo/schema-registry")
	if primary != schema {
		t.Fatalf("normalized Vault slots differ: primary=%+v schema=%+v", primary, schema)
	}
	trimmedNamespaceHeader := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
		Base: corectx.Base{
			VaultAddr:      "https://vault.example",
			VaultPath:      "service/prod",
			VaultNamespace: " \tteam-a\t ",
		},
	}, "vault", "alpha")
	if primary != trimmedNamespaceHeader {
		t.Fatal("Vault slot did not use the trimmed namespace value sent on the wire")
	}

	canonicalBasePath := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
		Base: corectx.Base{
			VaultAddr:      "https://vault.example/base~",
			VaultPath:      "service/prod",
			VaultNamespace: "team-a",
		},
	}, "vault", "alpha")
	for _, equivalentAddr := range []string{
		"HTTPS://VAULT.EXAMPLE:443/base%7E",
		"https://vault.example./root/../base~/",
	} {
		equivalent := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
			Base: corectx.Base{
				VaultAddr:      equivalentAddr,
				VaultPath:      "service/prod",
				VaultNamespace: "team-a",
			},
		}, "vault", "bravo")
		if canonicalBasePath != equivalent {
			t.Fatalf("equivalent Vault address %q produced slot %+v, want %+v", equivalentAddr, equivalent, canonicalBasePath)
		}
	}
	port8200 := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
		Base: corectx.Base{VaultAddr: "https://vault.example:8200/base~", VaultPath: "service/prod"},
	}, "vault", "alpha")
	port8201 := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
		Base: corectx.Base{VaultAddr: "https://vault.example:8201/base~", VaultPath: "service/prod"},
	}, "vault", "alpha")
	if port8200 == port8201 || port8200 == canonicalBasePath {
		t.Fatal("distinct non-default Vault ports collapsed to the same physical slot")
	}
	ipv6 := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
		Base: corectx.Base{VaultAddr: "https://[2001:0DB8:0:0::1]:443/base%7E", VaultPath: "service/prod"},
	}, "vault", "alpha")
	ipv6Canonical := mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
		Base: corectx.Base{VaultAddr: "https://[2001:db8::1]/base~", VaultPath: "service/prod"},
	}, "vault", "bravo")
	if ipv6 != ipv6Canonical || ipv6.vaultAddr != "https://[2001:db8::1]/base~" {
		t.Fatalf("IPv6 Vault identity = %+v, canonical = %+v", ipv6, ipv6Canonical)
	}
}

func TestVaultCredentialPhysicalSlotRejectsUnsafeAddressIdentity(t *testing.T) {
	for _, addr := range []string{
		"http://vault.example",
		"https://vault.example:",
		"https://vault.example:65536",
		"https://väult.example",
	} {
		t.Run(addr, func(t *testing.T) {
			_, err := contextCredentialPhysicalSlot(mqgovctx.Context{
				Base: corectx.Base{VaultAddr: addr, VaultPath: "service/prod"},
			}, "vault", "alpha")
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
				t.Fatalf("unsafe Vault address code = %s, want %s; err=%v", got, apperrors.CodeValidationFailed, err)
			}
		})
	}
	invalid := mqgovctx.Context{
		Base: corectx.Base{
			Password:          "secret",
			CredentialBackend: "vault",
			VaultAddr:         "https://vault.example:",
			VaultPath:         "service/prod",
		},
		Backend: "kafka",
	}
	if _, err := planContextImportCredential("alpha", &invalid); apperrors.AsAppError(err).Code != apperrors.CodeValidationFailed {
		t.Fatalf("planning unsafe Vault address error = %v, want %s", err, apperrors.CodeValidationFailed)
	}
	invalid.Password = credstore.EncodeRef("vault")
	if _, err := contextCredentialSlotOwners(map[string]mqgovctx.Context{"alpha": invalid}); apperrors.AsAppError(err).Code != apperrors.CodeValidationFailed {
		t.Fatalf("owner validation unsafe Vault address error = %v, want %s", err, apperrors.CodeValidationFailed)
	}
}

func mustContextCredentialPhysicalSlot(
	t *testing.T,
	item mqgovctx.Context,
	backendName string,
	key string,
) credentialPhysicalSlot {
	t.Helper()
	slot, err := contextCredentialPhysicalSlot(item, backendName, key)
	if err != nil {
		t.Fatalf("contextCredentialPhysicalSlot() error = %v", err)
	}
	return slot
}

func mustNewContextImportCredentialCandidate(
	t *testing.T,
	name string,
	item mqgovctx.Context,
) contextImportCredentialCandidate {
	t.Helper()
	candidate, err := newContextImportCredentialCandidate(name, item)
	if err != nil {
		t.Fatalf("newContextImportCredentialCandidate() error = %v", err)
	}
	return candidate
}

func TestVaultCredentialCandidateRejectsPhysicalSlotCollisions(t *testing.T) {
	backend := &migrationTestBackend{values: map[string]string{}}
	newCandidate := func(name, addr, path string) contextImportCredentialCandidate {
		candidate := mustNewContextImportCredentialCandidate(t, name, mqgovctx.Context{
			Base: corectx.Base{
				CredentialBackend: "vault",
				VaultAddr:         addr,
				VaultPath:         path,
				VaultNamespace:    "team-a",
			},
		})
		candidate.backend = backend
		return candidate
	}
	tests := []struct {
		name       string
		candidates []contextImportCredentialCandidate
	}{
		{
			name: "different contexts",
			candidates: func() []contextImportCredentialCandidate {
				alpha := newCandidate("alpha", " HTTPS://VAULT.EXAMPLE.:443/root/../shared%7E/ ", " /shared/path/ ")
				alpha.password = "alpha-secret"
				bravo := newCandidate("bravo", "https://vault.example/shared~", "shared/path")
				bravo.password = "bravo-secret"
				return []contextImportCredentialCandidate{alpha, bravo}
			}(),
		},
		{
			name: "same context primary and schema registry",
			candidates: func() []contextImportCredentialCandidate {
				candidate := newCandidate("alpha", "https://vault.example", "shared/path")
				candidate.password = "alpha-secret"
				candidate.schemaRegistryPassword = "schema-secret"
				return []contextImportCredentialCandidate{candidate}
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateContextImportCredentialCandidates(tt.candidates)
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
				t.Fatalf("collision code = %s, want %s; err=%v", got, apperrors.CodeValidationFailed, err)
			}
		})
	}
	if backend.putCalls != 0 || backend.deleteCalls != 0 || len(backend.values) != 0 {
		t.Fatalf("Vault collision touched credentials: puts=%d deletes=%d values=%v", backend.putCalls, backend.deleteCalls, backend.values)
	}
}

func TestVaultCredentialWriteRetainsPhysicalBackendIdentity(t *testing.T) {
	backend := &migrationTestBackend{values: map[string]string{}}
	candidate := mustNewContextImportCredentialCandidate(t, "alpha", mqgovctx.Context{
		Base: corectx.Base{
			CredentialBackend: "vault",
			VaultAddr:         "https://vault.example",
			VaultPath:         "shared/path",
		},
	})
	if candidate.primarySlot.backendName != "vault" {
		t.Fatalf("candidate physical backend = %q, want vault", candidate.primarySlot.backendName)
	}
	candidate.backend = backend
	candidate.password = "alpha-secret"
	transaction, err := storeContextImportCredentials(t.Context(), []contextImportCredentialCandidate{candidate})
	if err != nil {
		t.Fatalf("storeContextImportCredentials() error = %v", err)
	}
	if len(transaction.writes) != 1 || transaction.writes[0].slot.backendName != "vault" {
		t.Fatalf("transaction physical writes = %+v, want one Vault slot", transaction.writes)
	}
}

func TestVaultCredentialOwnersRejectPhysicalSlotCollisions(t *testing.T) {
	vaultContext := func(password, schemaPassword, addr, path string) mqgovctx.Context {
		return mqgovctx.Context{
			Base: corectx.Base{
				Password:          password,
				CredentialBackend: "vault",
				VaultAddr:         addr,
				VaultPath:         path,
				VaultNamespace:    "team-a",
			},
			Backend:                     "kafka",
			KafkaSchemaRegistryPassword: schemaPassword,
		}
	}
	tests := []struct {
		name     string
		cfg      *corectx.Config[mqgovctx.Context]
		expected map[string]mqgovctx.Context
	}{
		{
			name: "different contexts",
			cfg: &corectx.Config[mqgovctx.Context]{Contexts: map[string]mqgovctx.Context{
				"alpha": vaultContext(credstore.EncodeRef("vault"), "", "HTTPS://VAULT.EXAMPLE.:443/root/../base%7E/", "/shared/path/"),
			}},
			expected: map[string]mqgovctx.Context{
				"bravo": vaultContext(credstore.EncodeRef("vault"), "", "https://vault.example/base~", "shared/path"),
			},
		},
		{
			name: "same context primary and schema registry",
			cfg:  &corectx.Config[mqgovctx.Context]{Contexts: map[string]mqgovctx.Context{}},
			expected: map[string]mqgovctx.Context{
				"alpha": vaultContext(
					credstore.EncodeRef("vault"),
					credstore.EncodeRef("vault"),
					"https://vault.example",
					"shared/path",
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateContextImportCredentialKeySet(tt.cfg, tt.expected)
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeValidationFailed {
				t.Fatalf("owner collision code = %s, want %s; err=%v", got, apperrors.CodeValidationFailed, err)
			}
		})
	}
}

func TestVaultCredentialCompensationIsNotAttempted(t *testing.T) {
	t.Run("mixed transaction preflight", testMixedVaultCompensationNotAttempted)
	t.Run("ctx set restore", testCtxSetVaultCompensationNotAttempted)
	t.Run("ctx import delete", testCtxImportVaultCompensationNotAttempted)
}

func testMixedVaultCompensationNotAttempted(t *testing.T) {
	vaultBackend := &migrationTestBackend{values: map[string]string{"vault-key": "vault-written"}}
	normalBackend := &migrationTestBackend{values: map[string]string{"normal-key": "normal-written"}}
	transaction := &contextImportCredentialTransaction{writes: []contextImportCredentialWrite{
		{
			backend: vaultBackend,
			key:     "vault-key",
			slot: mustContextCredentialPhysicalSlot(t, mqgovctx.Context{
				Base: corectx.Base{VaultAddr: "https://vault.example", VaultPath: "shared/path"},
			}, "vault", "vault-key"),
			owner:        "vault-owner",
			previous:     "vault-previous",
			written:      "vault-written",
			existed:      true,
			putSucceeded: true,
		},
		{
			backend:      normalBackend,
			key:          "normal-key",
			slot:         mustContextCredentialPhysicalSlot(t, mqgovctx.Context{}, credentialBackendEncrypted, "normal-key"),
			owner:        "normal-owner",
			previous:     "normal-previous",
			written:      "normal-written",
			existed:      true,
			putSucceeded: true,
		},
	}}
	status, err := compensateContextImportCredentialsLocked(
		t.Context(),
		&corectx.Config[mqgovctx.Context]{Contexts: map[string]mqgovctx.Context{}},
		transaction,
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeCredentialStoreError {
		t.Fatalf("compensation code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, err)
	}
	if status != credentialCompensationNotSafe {
		t.Fatalf("compensation status = %q, want %q", status, credentialCompensationNotSafe)
	}
	assertBackendUnmodifiedByCompensation(t, vaultBackend, "vault-key", "vault-written")
	assertBackendUnmodifiedByCompensation(t, normalBackend, "normal-key", "normal-written")
}

func assertBackendUnmodifiedByCompensation(
	t *testing.T,
	backend *migrationTestBackend,
	key string,
	wantValue string,
) {
	t.Helper()
	if backend.putCalls != 0 || backend.deleteCalls != 0 {
		t.Fatalf("compensation mutated backend: puts=%d deletes=%d", backend.putCalls, backend.deleteCalls)
	}
	if got := backend.values[key]; got != wantValue {
		t.Fatalf("credential after refused compensation = %q, want %q", got, wantValue)
	}
}

func testCtxSetVaultCompensationNotAttempted(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	backend := &migrationTestBackend{
		values:                map[string]string{"dev": "previous-primary"},
		commitThenFailPutCall: 1,
	}
	candidate := mustNewContextImportCredentialCandidate(t, "dev", mqgovctx.Context{
		Base: corectx.Base{
			CredentialBackend: "vault",
			VaultAddr:         "https://vault.example",
			VaultPath:         "shared/path",
		},
	})
	candidate.backend = backend
	candidate.password = "new-primary"
	flags := newDefaultFlags()
	flags.commandCtx = t.Context()
	handle, err := beginMutationAudit(flags, mutationAuditSpec{
		Action:      "mq.ctx.set",
		ContextName: "dev",
		Target:      audit.EventTarget{ResourceType: "context", Resource: "dev"},
		Metadata:    mutationAuditMetadata{Items: 1},
	})
	if err != nil {
		t.Fatalf("beginMutationAudit() error = %v", err)
	}
	transaction, storeErr := storeContextImportCredentials(
		t.Context(),
		[]contextImportCredentialCandidate{candidate},
	)
	if got := apperrors.AsAppError(storeErr).Code; got != apperrors.CodeCredentialStoreError {
		t.Fatalf("credential store code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, storeErr)
	}
	status, compensationErr := compensateContextImportCredentialsLocked(
		t.Context(),
		&corectx.Config[mqgovctx.Context]{Contexts: map[string]mqgovctx.Context{}},
		transaction,
	)
	if status != credentialCompensationNotSafe || compensationErr == nil {
		t.Fatalf("Vault compensation status = %q, err=%v", status, compensationErr)
	}
	operationErr := contextSetCompensationError(storeErr, compensationErr)
	if err := finishContextImportAudit(handle, 1, status, true, operationErr); !errors.Is(err, operationErr) {
		t.Fatalf("finishContextImportAudit() error = %v, want operation error", err)
	}
	assertVaultCompensationNotAttempted(t, home, backend, "dev", "new-primary")
}

func testCtxImportVaultCompensationNotAttempted(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	configPath := filepath.Join(home, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	backend := &migrationTestBackend{
		values:                map[string]string{},
		commitThenFailPutCall: 1,
	}
	previousFactory := contextImportCredentialBackend
	contextImportCredentialBackend = func(mqgovctx.Context) (credstore.Backend, error) {
		return backend, nil
	}
	t.Cleanup(func() { contextImportCredentialBackend = previousFactory })
	flags := newDefaultFlags()
	flags.Yes = true
	flags.NonInter = true
	flags.Ticket = "OPS-1"
	flags.AllowContextChange = true
	flags.commandCtx = t.Context()
	doc := contextExportDocument{
		APIVersion: ctxExportAPIVersion,
		Name:       "imported",
		Context: &mqgovctx.Context{
			Base: corectx.Base{
				Password:          "import-secret",
				CredentialBackend: "vault",
				VaultAddr:         "https://vault.example/",
				VaultPath:         "/shared/path/",
				VaultNamespace:    "team-a",
			},
			Backend: "kafka",
		},
	}

	err := runCtxImportOne(flags, doc, ctxImportOptions{})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeCredentialStoreError {
		t.Fatalf("ctx import code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, err)
	}
	assertVaultCompensationNotAttempted(t, home, backend, "imported", "import-secret")
	cfg, loadErr := mqgovctx.Load()
	if loadErr != nil {
		t.Fatalf("Load() error = %v", loadErr)
	}
	if _, exists := cfg.Contexts["imported"]; exists {
		t.Fatal("failed Vault ctx import persisted its context")
	}
}

func assertVaultCompensationNotAttempted(
	t *testing.T,
	home string,
	backend *migrationTestBackend,
	key string,
	wantValue string,
) {
	t.Helper()
	if backend.putCalls != 1 || backend.deleteCalls != 0 {
		t.Fatalf("Vault compensation performed a mutation: puts=%d deletes=%d", backend.putCalls, backend.deleteCalls)
	}
	if got := backend.values[key]; got != wantValue {
		t.Fatalf("Vault value after refused compensation = %q, want %q", got, wantValue)
	}
	records := readSafeAuditRecords(t, filepath.Join(home, ".mqgov-cli", "audit.log"))
	if len(records) < 2 {
		t.Fatalf("audit records = %d, want intent and outcome", len(records))
	}
	last := records[len(records)-1]
	if last.Status != audit.StatusFailed || last.Outcome == nil {
		t.Fatalf("Vault compensation audit record = %+v", last)
	}
	if last.Outcome.Succeeded != 0 ||
		last.Outcome.Failed != 0 ||
		last.Outcome.Uncertain != 1 ||
		last.Outcome.CompensationStatus != credentialCompensationNotSafe {
		t.Fatalf("Vault compensation audit outcome = %+v", last.Outcome)
	}
}

func assertFailedContextSetCompensated(
	t *testing.T,
	home string,
	backend *migrationTestBackend,
	configPath string,
	wantValues map[string]string,
) {
	t.Helper()
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if _, exists := cfg.Contexts["dev"]; exists {
		t.Fatalf("failed ctx set persisted context in %s: %+v", configPath, cfg.Contexts["dev"])
	}
	if len(backend.values) != len(wantValues) {
		t.Fatalf("credential compensation state = %v, want %v", backend.values, wantValues)
	}
	for key, want := range wantValues {
		if backend.values[key] != want {
			t.Fatalf("credential compensation state = %v, want %v", backend.values, wantValues)
		}
	}
	auditData, err := os.ReadFile(filepath.Join(home, ".mqgov-cli", "audit.log"))
	if err != nil {
		t.Fatalf("ReadFile(audit.log) error = %v", err)
	}
	auditText := string(auditData)
	if !strings.Contains(auditText, `"compensationStatus":"succeeded"`) {
		t.Fatalf("audit omitted successful compensation: %s", auditText)
	}
	if strings.Contains(auditText, `"uncertain":1`) {
		t.Fatalf("successful compensation reported uncertainty: %s", auditText)
	}
}

func TestCtxSetStoresRabbitMQUsername(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })

	out, err := runCommandForTest(t,
		"-o", "json",
		"--config", configPath,
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"--backend", "rabbitmq",
		"ctx", "set", "dev",
		"--host", "127.0.0.1",
		"--port", "5672",
		"--vhost", "/",
		"--management-url", "http://127.0.0.1:15672",
		"--username", "guest",
	)
	if err != nil {
		t.Fatalf("ctx set rabbitmq error = %v; out=%s", err, out)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	item := cfg.Contexts["dev"]
	if item.Username != "guest" {
		t.Fatalf("RabbitMQ username = %q, want guest", item.Username)
	}
	if item.RabbitMQHost != "127.0.0.1" || item.RabbitMQPort != 5672 || item.RabbitMQVHost != "/" {
		t.Fatalf("RabbitMQ context fields = %+v", item)
	}
}

func TestCtxAddedSubcommandHelp(t *testing.T) {
	tests := [][]string{
		{"ctx", "export", "--help"},
		{"ctx", "import", "--help"},
		{"ctx", "role", "--help"},
		{"ctx", "migrate-credentials", "--help"},
	}
	for _, args := range tests {
		out, err := runCommandForTest(t, args...)
		if err != nil {
			t.Fatalf("%v error = %v; out=%s", args, err, out)
		}
		if !strings.Contains(out, "Usage:") {
			t.Fatalf("%v help missing Usage: %s", args, out)
		}
	}
}

func TestCtxExportRedactsCredentialsByDefault(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{
		Base:                        corectx.Base{Password: "secret", CredentialBackend: "plain-yaml"},
		Backend:                     "kafka",
		KafkaBrokers:                []string{"127.0.0.1:9092"},
		KafkaSchemaRegistryPassword: "sr-secret",
		RabbitMQAMQPURL:             "amqp://user:url-pass@localhost:5672/",
	}); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "--config", configPath, "ctx", "export", "dev")
	if err != nil {
		t.Fatalf("ctx export error = %v; out=%s", err, out)
	}
	for _, forbidden := range []string{"secret", "sr-secret", "url-pass"} {
		if strings.Contains(out, forbidden) {
			t.Fatalf("ctx export leaked %q: %s", forbidden, out)
		}
	}
	if !strings.Contains(out, redactedCredential) || !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("ctx export missing redaction markers: %s", out)
	}
}

func TestCtxExportAllOutputAndImportInputOverwrite(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{Backend: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}}); err != nil {
		t.Fatal(err)
	}
	exportPath := filepath.Join(dir, "contexts.yaml")
	if out, err := runCommandForTest(t, "--config", configPath, "ctx", "export", "--all", "--output", exportPath); err != nil {
		t.Fatalf("ctx export --all error = %v; out=%s", err, out)
	}
	data, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "contexts:") || !strings.Contains(string(data), "dev:") {
		t.Fatalf("export --all output missing contexts: %s", data)
	}

	importPath := filepath.Join(dir, "imported.yaml")
	out, err := runCommandForTest(t,
		"--config", importPath,
		"-o", "json",
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"ctx", "import",
		"--input", exportPath,
		"--overwrite",
	)
	if err != nil {
		t.Fatalf("ctx import --input error = %v; out=%s", err, out)
	}
	if !strings.Contains(out, `"kind": "ContextImportResult"`) || !strings.Contains(out, `"count": 1`) {
		t.Fatalf("import output = %s", out)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contexts["dev"].Backend != "kafka" {
		t.Fatalf("imported context = %+v", cfg.Contexts["dev"])
	}
}

func TestCtxExportRejectsGovernedStateOutputPaths(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{Backend: "fake"}); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := runCommandForTest(
		t,
		"--config", configPath,
		"ctx", "export", "dev",
		"--output", configPath,
	); apperrors.AsAppError(err).Code != apperrors.CodeUsageError {
		t.Fatalf("same-path export error = %v, want USAGE_ERROR", err)
	}

	hardlinkPath := filepath.Join(dir, "config-hardlink.yaml")
	if err := os.Link(configPath, hardlinkPath); err == nil {
		if _, exportErr := runCommandForTest(
			t,
			"--config", configPath,
			"ctx", "export", "dev",
			"--output", hardlinkPath,
		); apperrors.AsAppError(exportErr).Code != apperrors.CodeUsageError {
			t.Fatalf("hardlink export error = %v, want USAGE_ERROR", exportErr)
		}
	}

	symlinkPath := filepath.Join(dir, "config-symlink.yaml")
	if err := os.Symlink(configPath, symlinkPath); err == nil {
		if _, exportErr := runCommandForTest(
			t,
			"--config", configPath,
			"ctx", "export", "dev",
			"--output", symlinkPath,
		); apperrors.AsAppError(exportErr).Code != apperrors.CodeLocalIOError {
			t.Fatalf("symlink export error = %v, want LOCAL_IO_ERROR", exportErr)
		}
	}

	home := t.TempDir()
	auditPath := filepath.Join(home, ".mqgov-cli", "audit.log")
	for name, target := range map[string]string{
		"config temp":     configPath + ".tmp",
		"credential temp": filepath.Join(home, ".mqgov-cli", "credentials.enc.tmp"),
		"audit key":       auditPath + ".hmac-key",
		"checkpoint temp": auditPath + ".checkpoint.tmp-attacker",
		"case variant": filepath.Join(
			filepath.Dir(auditPath),
			strings.ToUpper(filepath.Base(auditPath))+".CHECKPOINT.TMP-ATTACKER",
		),
	} {
		t.Run(name, func(t *testing.T) {
			if _, exportErr := runCommandForTestAtHome(
				t,
				home,
				"--config", configPath,
				"ctx", "export", "dev",
				"--output", target,
			); apperrors.AsAppError(exportErr).Code != apperrors.CodeUsageError {
				t.Fatalf("protected artifact export error = %v, want USAGE_ERROR", exportErr)
			}
		})
	}

	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(original) {
		t.Fatalf("rejected export changed context config:\nbefore=%s\nafter=%s", original, after)
	}
}

func TestCtxExportIncludeCredentialsAtomicallyReplacesWithOwnerOnlyFile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{
		Base:    corectx.Base{Password: "export-secret", CredentialBackend: "plain-yaml"},
		Backend: "fake",
	}); err != nil {
		t.Fatal(err)
	}
	exportPath := filepath.Join(dir, "contexts.yaml")
	if err := os.WriteFile(exportPath, []byte("old-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(exportPath, 0o644); err != nil {
		t.Fatal(err)
	}

	if out, err := runCommandForTest(
		t,
		"--config", configPath,
		"ctx", "export", "dev",
		"--include-credentials",
		"--output", exportPath,
	); err != nil {
		t.Fatalf("ctx export --include-credentials error = %v; out=%s", err, out)
	}
	data, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "export-secret") || strings.Contains(string(data), "old-content") {
		t.Fatalf("context export content = %s", data)
	}
	if err := verifyContextExportOwnerOnly(exportPath); err != nil {
		t.Fatalf("credential-bearing export is not owner-only: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".mqgov-context-export-*.tmp"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary export files = %v, error = %v", matches, err)
	}
}

func TestCtxRoleLifecycle(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Set("dev", mqgovctx.Context{
		Base:    corectx.Base{Roles: map[string]string{operator: safety.RoleAdmin}},
		Backend: "fake",
	}); err != nil {
		t.Fatal(err)
	}

	flags := newDefaultFlags()
	flags.Yes = true
	flags.NonInter = true
	flags.Ticket = "OPS-1"
	flags.AllowRoleChange = true
	if err := runCtxRoleSet(flags, "dev", roleOptions{targetOperator: "alice", role: safety.RoleReader}); err != nil {
		t.Fatalf("runCtxRoleSet error = %v", err)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contexts["dev"].Roles["alice"] != safety.RoleReader {
		t.Fatalf("roles = %#v", cfg.Contexts["dev"].Roles)
	}
	if err := runCtxRoleUnset(flags, "dev", roleOptions{targetOperator: "alice"}); err != nil {
		t.Fatalf("runCtxRoleUnset error = %v", err)
	}
	cfg, err = mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Contexts["dev"].Roles) != 1 || cfg.Contexts["dev"].Roles[operator] != safety.RoleAdmin {
		t.Fatalf("roles after unset = %#v, want only trusted admin", cfg.Contexts["dev"].Roles)
	}
}

func TestCtxMigrateCredentialsDryRunCountsPrimaryAndSchemaRegistry(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("dev", mqgovctx.Context{
		Base:                        corectx.Base{Password: "secret"},
		Backend:                     "kafka",
		KafkaBrokers:                []string{"127.0.0.1:9092"},
		KafkaSchemaRegistryPassword: "sr-secret",
	}); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "--config", configPath, "-o", "json", "ctx", "migrate-credentials", "--dry-run")
	if err != nil {
		t.Fatalf("ctx migrate-credentials --dry-run error = %v; out=%s", err, out)
	}
	for _, want := range []string{`"kind": "CredentialMigration"`, `"dryRun": true`, `"credentials": 2`, `"dev"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("migrate dry-run output missing %q: %s", want, out)
		}
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contexts["dev"].Password != "secret" || cfg.Contexts["dev"].KafkaSchemaRegistryPassword != "sr-secret" {
		t.Fatalf("dry-run mutated credentials: %+v", cfg.Contexts["dev"])
	}
}

func TestCredentialMigrationTracksExactProgressAndCompensates(t *testing.T) {
	backend := &migrationTestBackend{
		values: map[string]string{
			"alpha": "previous-alpha",
		},
		failPutCall: 2,
	}
	candidates := []migrateCredentialCandidate{
		{name: "alpha", password: "new-alpha", schemaRegistryPassword: "new-schema"},
		{name: "bravo", password: "new-bravo"},
	}
	transaction, progress, err := storeCredentialMigrationCandidates(t.Context(), backend, candidates)
	if apperrors.AsAppError(err).Code != apperrors.CodeCredentialStoreError {
		t.Fatalf("storeCredentialMigrationCandidates() error = %v, want CREDENTIAL_STORE_ERROR", err)
	}
	skipped := credentialMigrationCredentialCount(candidates) - progress.succeeded - progress.failed
	if progress.succeeded != 1 || progress.failed != 1 || skipped != 1 {
		t.Fatalf("migration progress = %+v skipped=%d, want succeeded=1 failed=1 skipped=1", progress, skipped)
	}
	status, compensationErr := compensateCredentialMigrationTransaction(t.Context(), transaction)
	if compensationErr != nil {
		t.Fatalf("credential compensation error = %v", compensationErr)
	}
	if status != credentialCompensationSucceeded {
		t.Fatalf("credential compensation status = %q, want %q", status, credentialCompensationSucceeded)
	}
	if got := backend.values["alpha"]; got != "previous-alpha" {
		t.Fatalf("compensation restored alpha = %q, want previous-alpha", got)
	}
	if _, exists := backend.values["alpha/schema-registry"]; exists {
		t.Fatal("compensation left a newly-created schema registry credential")
	}
	if _, exists := backend.values["bravo"]; exists {
		t.Fatal("failed credential write unexpectedly persisted")
	}
	finalProgress := progress.afterSuccessfulCompensation()
	if finalProgress.succeeded != 0 || finalProgress.failed != 2 {
		t.Fatalf("compensated progress = %+v, want succeeded=0 failed=2", finalProgress)
	}

	original := &corectx.Config[mqgovctx.Context]{
		Contexts: map[string]mqgovctx.Context{
			"alpha": candidates[0].context,
			"bravo": candidates[1].context,
		},
	}
	if committed, unchanged := credentialMigrationConfigState(original, candidates, credentialBackendEncrypted); committed || !unchanged {
		t.Fatalf("original config state = committed:%t unchanged:%t", committed, unchanged)
	}
	migrated := &corectx.Config[mqgovctx.Context]{
		Contexts: map[string]mqgovctx.Context{},
	}
	for _, candidate := range candidates {
		item := candidate.context
		if candidate.password != "" {
			item.Password = credstore.EncodeRef(credentialBackendEncrypted)
		}
		if candidate.schemaRegistryPassword != "" {
			item.KafkaSchemaRegistryPassword = credstore.EncodeRef(credentialBackendEncrypted)
		}
		item.CredentialBackend = credentialBackendEncrypted
		migrated.Contexts[candidate.name] = item
	}
	if committed, unchanged := credentialMigrationConfigState(migrated, candidates, credentialBackendEncrypted); !committed || unchanged {
		t.Fatalf("migrated config state = committed:%t unchanged:%t", committed, unchanged)
	}
}

func TestCredentialMigrationCompensatesPutThatCommittedThenErrored(t *testing.T) {
	tests := []struct {
		name             string
		failCompensation bool
		wantStatus       string
		wantValue        string
	}{
		{
			name:       "restore succeeds",
			wantStatus: credentialCompensationSucceeded,
			wantValue:  "previous-alpha",
		},
		{
			name:             "restore fails",
			failCompensation: true,
			wantStatus:       credentialCompensationIncomplete,
			wantValue:        "new-alpha",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &migrationTestBackend{
				values:                map[string]string{"alpha": "previous-alpha"},
				commitThenFailPutCall: 1,
			}
			if tt.failCompensation {
				backend.failPutCall = 2
			}
			candidates := []migrateCredentialCandidate{{name: "alpha", password: "new-alpha"}}
			transaction, progress, err := storeCredentialMigrationCandidates(t.Context(), backend, candidates)
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeCredentialStoreError {
				t.Fatalf("storeCredentialMigrationCandidates() code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, err)
			}
			if progress.succeeded != 0 || progress.failed != 1 || progress.uncertain != 0 {
				t.Fatalf("migration progress after indeterminate Put = %+v", progress)
			}
			if len(transaction.writes) != 1 ||
				transaction.writes[0].written != "new-alpha" ||
				transaction.writes[0].putSucceeded {
				t.Fatalf("failed Put was not retained as an attempted write: %+v", transaction.writes)
			}
			if got := backend.values["alpha"]; got != "new-alpha" {
				t.Fatalf("post-commit Put value = %q, want new-alpha", got)
			}

			status, compensationErr := compensateCredentialMigrationTransaction(t.Context(), transaction)
			if status != tt.wantStatus {
				t.Fatalf("compensation status = %q, want %q; err=%v", status, tt.wantStatus, compensationErr)
			}
			assertCredentialMigrationCompensationProgress(
				t,
				progress,
				tt.failCompensation,
				compensationErr,
			)
			if got := backend.values["alpha"]; got != tt.wantValue {
				t.Fatalf("credential value after compensation = %q, want %q", got, tt.wantValue)
			}
		})
	}
}

func assertCredentialMigrationCompensationProgress(
	t *testing.T,
	progress credentialMigrationProgress,
	failCompensation bool,
	compensationErr error,
) {
	t.Helper()
	if failCompensation && compensationErr == nil {
		t.Fatal("failed compensation returned no error")
	}
	if !failCompensation && compensationErr != nil {
		t.Fatalf("compensation error = %v", compensationErr)
	}
	want := credentialMigrationProgress{failed: 1}
	if failCompensation {
		progress = progress.afterIncompleteCompensation()
		want = credentialMigrationProgress{uncertain: 1}
	} else {
		progress = progress.afterSuccessfulCompensation()
	}
	if progress != want {
		t.Fatalf("compensation progress = %+v, want %+v", progress, want)
	}
}

func TestCredentialMigrationReconciliationRefusesCredentialTakeover(t *testing.T) {
	tests := []struct {
		name          string
		ownerTakeover bool
	}{
		{name: "value takeover"},
		{name: "owner takeover", ownerTakeover: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			mqgovctx.SetConfigPath(configPath)
			t.Cleanup(func() { mqgovctx.SetConfigPath("") })
			if err := mqgovctx.Set("foo", mqgovctx.Context{
				Backend:                     "kafka",
				KafkaSchemaRegistryPassword: "new-schema",
			}); err != nil {
				t.Fatal(err)
			}
			cfg, err := mqgovctx.Load()
			if err != nil {
				t.Fatal(err)
			}
			candidate := migrateCredentialCandidate{
				name:                   "foo",
				context:                cfg.Contexts["foo"],
				schemaRegistryPassword: "new-schema",
			}
			credentialKey := "foo/schema-registry"
			backend := &migrationTestBackend{values: map[string]string{credentialKey: "takeover-value"}}
			transaction := &credentialMigrationTransaction{
				backend: backend,
				writes: []credentialMigrationWrite{{
					key:          credentialKey,
					slot:         mustContextCredentialPhysicalSlot(t, mqgovctx.Context{}, credentialBackendEncrypted, credentialKey),
					owner:        "foo\x00schema-registry",
					written:      "new-schema",
					putSucceeded: true,
				}},
			}
			if tt.ownerTakeover {
				if err := mqgovctx.Set(credentialKey, mqgovctx.Context{
					Base:    corectx.Base{Password: credstore.EncodeRef(credentialBackendEncrypted)},
					Backend: "kafka",
				}); err != nil {
					t.Fatal(err)
				}
			}

			progress, status, reconcileErr := reconcileCredentialMigrationFailure(
				t.Context(),
				transaction,
				credentialMigrationProgress{succeeded: 1},
				[]migrateCredentialCandidate{candidate},
				credentialBackendEncrypted,
			)
			if got := apperrors.AsAppError(reconcileErr).Code; got != apperrors.CodeCredentialStoreError {
				t.Fatalf("reconciliation code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, reconcileErr)
			}
			if status != credentialCompensationNotSafe {
				t.Fatalf("compensation status = %q, want %q", status, credentialCompensationNotSafe)
			}
			if progress.succeeded != 0 || progress.failed != 0 || progress.uncertain != 1 {
				t.Fatalf("takeover reconciliation progress = %+v, want uncertain=1", progress)
			}
			if got := backend.values[credentialKey]; got != "takeover-value" {
				t.Fatalf("credential takeover value = %q, want preserved takeover-value", got)
			}
			cfg, err = mqgovctx.Load()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg.Contexts["foo"], candidate.context) {
				t.Fatalf("migration reconciliation changed source context: got=%+v want=%+v", cfg.Contexts["foo"], candidate.context)
			}
			if tt.ownerTakeover {
				if _, exists := cfg.Contexts[credentialKey]; !exists {
					t.Fatalf("owner takeover context %q was lost", credentialKey)
				}
			}
		})
	}
}

func TestContextImportValidatesAllCredentialBackendsBeforeWriting(t *testing.T) {
	backend := &migrationTestBackend{
		values:            map[string]string{},
		failAvailableCall: 2,
	}
	alpha := mustNewContextImportCredentialCandidate(t, "alpha", mqgovctx.Context{
		Base: corectx.Base{CredentialBackend: backend.Name()},
	})
	alpha.backend = backend
	alpha.password = "alpha-secret"
	bravo := mustNewContextImportCredentialCandidate(t, "bravo", mqgovctx.Context{
		Base: corectx.Base{CredentialBackend: backend.Name()},
	})
	bravo.backend = backend
	bravo.password = "bravo-secret"
	candidates := []contextImportCredentialCandidate{alpha, bravo}

	err := validateContextImportCredentialCandidates(candidates)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeCredentialStoreError {
		t.Fatalf("validateContextImportCredentialCandidates() code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, err)
	}
	if backend.putCalls != 0 || len(backend.values) != 0 {
		t.Fatalf("validate-first wrote credentials: calls=%d values=%v", backend.putCalls, backend.values)
	}
}

func TestContextImportConfigStateDistinguishesCommittedUnchangedAndMixed(t *testing.T) {
	originalAlpha := mqgovctx.Context{
		Backend:      "kafka",
		KafkaBrokers: []string{"old:9092"},
	}
	expected := map[string]mqgovctx.Context{
		"alpha": {
			Backend:      "kafka",
			KafkaBrokers: []string{"new:9092"},
		},
		"bravo": {
			Backend:      "kafka",
			KafkaBrokers: []string{"bravo:9092"},
		},
	}
	original := map[string]contextImportTargetState{
		"alpha": {context: originalAlpha, exists: true},
		"bravo": {},
	}

	unchangedConfig := &corectx.Config[mqgovctx.Context]{
		Contexts: map[string]mqgovctx.Context{"alpha": originalAlpha},
	}
	if committed, unchanged := contextImportConfigState(unchangedConfig, original, expected); committed || !unchanged {
		t.Fatalf("unchanged state = committed:%t unchanged:%t", committed, unchanged)
	}

	committedConfig := &corectx.Config[mqgovctx.Context]{
		Contexts: map[string]mqgovctx.Context{
			"alpha": expected["alpha"],
			"bravo": expected["bravo"],
		},
	}
	if committed, unchanged := contextImportConfigState(committedConfig, original, expected); !committed || unchanged {
		t.Fatalf("committed state = committed:%t unchanged:%t", committed, unchanged)
	}

	mixedConfig := &corectx.Config[mqgovctx.Context]{
		Contexts: map[string]mqgovctx.Context{
			"alpha": expected["alpha"],
		},
	}
	if committed, unchanged := contextImportConfigState(mixedConfig, original, expected); committed || unchanged {
		t.Fatalf("mixed state = committed:%t unchanged:%t", committed, unchanged)
	}
}

func TestMultiContextImportCompensatesPartialCredentialWrites(t *testing.T) {
	testContextImportCredentialFailure(t, false, "succeeded", `"uncertain"`)
}

func TestMultiContextImportReportsUncertainWhenCompensationFails(t *testing.T) {
	testContextImportCredentialFailure(t, true, "incomplete", `"uncertain":2`)
}

func testContextImportCredentialFailure(
	t *testing.T,
	failCompensation bool,
	wantCompensationStatus string,
	forbiddenOrRequiredUncertain string,
) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	configPath := filepath.Join(home, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	backend := &migrationTestBackend{
		values: map[string]string{
			"alpha": "previous-alpha",
		},
		failPutCall:     2,
		failPutFromCall: 0,
	}
	if failCompensation {
		backend.failPutCall = 0
		backend.failPutFromCall = 2
	}
	previousFactory := contextImportCredentialBackend
	contextImportCredentialBackend = func(mqgovctx.Context) (credstore.Backend, error) {
		return backend, nil
	}
	t.Cleanup(func() { contextImportCredentialBackend = previousFactory })
	doc := contextExportDocument{
		APIVersion: ctxExportAPIVersion,
		Contexts: map[string]mqgovctx.Context{
			"alpha": {
				Base:         corectx.Base{Password: "alpha-secret", CredentialBackend: backend.Name()},
				Backend:      "kafka",
				KafkaBrokers: []string{"127.0.0.1:9092"},
			},
			"bravo": {
				Base:         corectx.Base{Password: "bravo-secret", CredentialBackend: backend.Name()},
				Backend:      "kafka",
				KafkaBrokers: []string{"127.0.0.1:9092"},
			},
		},
	}
	flags := newDefaultFlags()
	flags.Yes = true
	flags.NonInter = true
	flags.Ticket = "OPS-1"
	flags.AllowContextChange = true
	flags.commandCtx = t.Context()

	err := runCtxImportMany(flags, doc, ctxImportOptions{})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeCredentialStoreError {
		t.Fatalf("runCtxImportMany() code = %s, want %s; err=%v", got, apperrors.CodeCredentialStoreError, err)
	}
	cfg, loadErr := mqgovctx.Load()
	if loadErr != nil {
		t.Fatalf("Load() error = %v", loadErr)
	}
	if len(cfg.Contexts) != 0 {
		t.Fatalf("failed import persisted contexts: %+v", cfg.Contexts)
	}
	if failCompensation {
		if backend.values["alpha"] != "alpha-secret" {
			t.Fatalf("failed compensation state = %v, want explicit uncertain alpha write", backend.values)
		}
	} else if len(backend.values) != 1 || backend.values["alpha"] != "previous-alpha" {
		t.Fatalf("compensated import did not restore credential state: %v", backend.values)
	}
	auditData, readErr := os.ReadFile(filepath.Join(home, ".mqgov-cli", "audit.log"))
	if readErr != nil {
		t.Fatalf("ReadFile(audit.log) error = %v", readErr)
	}
	auditText := string(auditData)
	if !strings.Contains(auditText, `"compensationStatus":"`+wantCompensationStatus+`"`) {
		t.Fatalf("audit missing compensation status %q: %s", wantCompensationStatus, auditText)
	}
	if failCompensation {
		if !strings.Contains(auditText, forbiddenOrRequiredUncertain) {
			t.Fatalf("audit missing uncertainty %q: %s", forbiddenOrRequiredUncertain, auditText)
		}
	} else if strings.Contains(auditText, forbiddenOrRequiredUncertain) {
		t.Fatalf("successful compensation reported uncertainty: %s", auditText)
	}
}

func TestCredentialMigrationCompensationIgnoresParentCancellationWithDeadline(t *testing.T) {
	backend := &migrationTestBackend{values: map[string]string{"alpha": "new-alpha"}}
	transaction := &credentialMigrationTransaction{
		backend: backend,
		writes: []credentialMigrationWrite{{
			key:          "alpha",
			slot:         mustContextCredentialPhysicalSlot(t, mqgovctx.Context{}, backend.Name(), "alpha"),
			owner:        "alpha\x00primary",
			written:      "new-alpha",
			putSucceeded: true,
		}},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := compensateCredentialMigration(ctx, transaction); err != nil {
		t.Fatalf("compensateCredentialMigration() error = %v", err)
	}
	if backend.deleteContextErr != nil {
		t.Fatalf("compensation inherited cancellation: %v", backend.deleteContextErr)
	}
	if !backend.deleteContextHasDeadline {
		t.Fatal("compensation context has no bounded deadline")
	}
	if _, exists := backend.values["alpha"]; exists {
		t.Fatal("compensation did not remove the newly-written credential")
	}
}

func TestCredentialMigrationKeySetRejectsPlannedAndExistingCollisions(t *testing.T) {
	tests := []struct {
		name      string
		contexts  map[string]mqgovctx.Context
		wantError bool
	}{
		{
			name: "two planned slots",
			contexts: map[string]mqgovctx.Context{
				"foo": {
					KafkaSchemaRegistryPassword: "new-schema",
				},
				"foo/schema-registry": {
					Base: corectx.Base{Password: "new-primary"},
				},
			},
			wantError: true,
		},
		{
			name: "planned slot collides with existing target backend ref",
			contexts: map[string]mqgovctx.Context{
				"foo": {
					KafkaSchemaRegistryPassword: "new-schema",
				},
				"foo/schema-registry": {
					Base: corectx.Base{Password: credstore.EncodeRef(credentialBackendEncrypted)},
				},
			},
			wantError: true,
		},
		{
			name: "different backend namespace",
			contexts: map[string]mqgovctx.Context{
				"foo": {
					KafkaSchemaRegistryPassword: "new-schema",
				},
				"foo/schema-registry": {
					Base: corectx.Base{Password: credstore.EncodeRef(credentialBackendKeychain)},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &corectx.Config[mqgovctx.Context]{Contexts: tt.contexts}
			candidates, err := credentialMigrationCandidates(cfg, "")
			if err != nil {
				t.Fatal(err)
			}
			err = validateCredentialMigrationKeySet(cfg, candidates, credentialBackendEncrypted)
			if tt.wantError && apperrors.AsAppError(err).Code != apperrors.CodeValidationFailed {
				t.Fatalf("collision error = %v, want VALIDATION_FAILED", err)
			}
			if !tt.wantError && err != nil {
				t.Fatalf("unexpected collision error = %v", err)
			}
		})
	}
}

func TestRabbitMQUsesMQGOVPasswordForCurrentContext(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	t.Setenv("MQGOV_PASSWORD", "env-secret")
	server := rabbitMQManagementAuthServer(t, "rabbit-user", "env-secret")
	defer server.Close()

	if err := mqgovctx.Set("prod", mqgovctx.Context{
		Base:                  corectx.Base{Username: "rabbit-user"},
		Backend:               "rabbitmq",
		RabbitMQManagementURL: server.URL,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("prod"); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "--config", configPath, "-o", "json", "topic", "list")
	if err != nil {
		t.Fatalf("topic list with MQGOV_PASSWORD error = %v; out=%s", err, out)
	}
}

func TestRabbitMQUsesMQGOVPasswordForContextOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	t.Setenv("MQGOV_PASSWORD", "override-secret")
	server := rabbitMQManagementAuthServer(t, "prod-user", "override-secret")
	defer server.Close()

	if err := mqgovctx.Set("dev", mqgovctx.Context{
		Base:                  corectx.Base{Username: "dev-user", Password: "dev-secret"},
		Backend:               "rabbitmq",
		RabbitMQManagementURL: "http://127.0.0.1:1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Set("prod", mqgovctx.Context{
		Base:                  corectx.Base{Username: "prod-user"},
		Backend:               "rabbitmq",
		RabbitMQManagementURL: server.URL,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("dev"); err != nil {
		t.Fatal(err)
	}

	out, err := runCommandForTest(t, "--config", configPath, "--context", "prod", "-o", "json", "topic", "list")
	if err != nil {
		t.Fatalf("topic list with --context prod and MQGOV_PASSWORD error = %v; out=%s", err, out)
	}
}

func rabbitMQManagementAuthServer(t *testing.T, wantUser, wantPassword string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != wantUser || password != wantPassword {
			t.Errorf("BasicAuth() = %q/%q ok=%t, want %q/%q", user, password, ok, wantUser, wantPassword)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/api/queues/%2F" {
			t.Errorf("request = %s %s, want GET /api/queues/%%2F", r.Method, r.URL.EscapedPath())
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
}
