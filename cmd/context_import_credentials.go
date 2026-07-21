package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"net/url"
	"path"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/credstore"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

type contextImportCredentialCandidate struct {
	name                   string
	backend                credstore.Backend
	password               string
	schemaRegistryPassword string
	primarySlot            credentialPhysicalSlot
	schemaRegistrySlot     credentialPhysicalSlot
}

type contextImportCredentialWrite struct {
	backend      credstore.Backend
	key          string
	slot         credentialPhysicalSlot
	owner        string
	previous     string
	written      string
	existed      bool
	putSucceeded bool
}

type contextImportCredentialTransaction struct {
	writes []contextImportCredentialWrite
}

type contextImportTargetState struct {
	context mqgovctx.Context
	exists  bool
}

type credentialPhysicalSlot struct {
	backendName    string
	key            string
	vaultAddr      string
	vaultNamespace string
	vaultPath      string
}

var contextImportCredentialBackend = credentialBackendForContext

const (
	credentialCompensationSucceeded  = "succeeded"
	credentialCompensationIncomplete = "incomplete"
	credentialCompensationNotSafe    = "not-safe"
)

func planContextImportCredential(
	name string,
	item *mqgovctx.Context,
) (contextImportCredentialCandidate, error) {
	if item.CredentialBackend == "" {
		if ref := credstore.ParseRef(item.Password); ref.IsRef {
			item.CredentialBackend = ref.BackendName
		}
		if ref := credstore.ParseRef(item.KafkaSchemaRegistryPassword); ref.IsRef {
			item.CredentialBackend = ref.BackendName
		}
	}
	candidate, err := newContextImportCredentialCandidate(name, *item)
	if err != nil {
		return contextImportCredentialCandidate{}, err
	}
	if item.CredentialBackend == "" || item.CredentialBackend == "plain-yaml" {
		return candidate, nil
	}
	backend, err := contextImportCredentialBackend(*item)
	if err != nil {
		return contextImportCredentialCandidate{}, err
	}
	candidate.backend = backend
	if isLiteralCredential(item.Password) {
		candidate.password = item.Password
		item.Password = credstore.EncodeRef(item.CredentialBackend)
	}
	if isLiteralCredential(item.KafkaSchemaRegistryPassword) {
		candidate.schemaRegistryPassword = item.KafkaSchemaRegistryPassword
		item.KafkaSchemaRegistryPassword = credstore.EncodeRef(item.CredentialBackend)
	}
	return candidate, nil
}

func newContextImportCredentialCandidate(
	name string,
	item mqgovctx.Context,
) (contextImportCredentialCandidate, error) {
	primarySlot, err := contextCredentialPhysicalSlot(item, item.CredentialBackend, name)
	if err != nil {
		return contextImportCredentialCandidate{}, err
	}
	schemaRegistrySlot, err := contextCredentialPhysicalSlot(
		item,
		item.CredentialBackend,
		name+"/schema-registry",
	)
	if err != nil {
		return contextImportCredentialCandidate{}, err
	}
	return contextImportCredentialCandidate{
		name:               name,
		primarySlot:        primarySlot,
		schemaRegistrySlot: schemaRegistrySlot,
	}, nil
}

func contextCredentialPhysicalSlot(
	item mqgovctx.Context,
	backendName string,
	key string,
) (credentialPhysicalSlot, error) {
	if backendName == "vault" {
		addr, err := canonicalVaultAddrIdentity(item.VaultAddr)
		if err != nil {
			return credentialPhysicalSlot{}, err
		}
		return credentialPhysicalSlot{
			backendName:    backendName,
			vaultAddr:      addr,
			vaultNamespace: textproto.TrimString(item.VaultNamespace),
			vaultPath:      strings.Trim(strings.TrimSpace(item.VaultPath), "/"),
		}, nil
	}
	return credentialPhysicalSlot{backendName: backendName, key: key}, nil
}

func canonicalVaultAddrIdentity(rawAddr string) (string, error) { //nolint:gocyclo // URL identity normalization is a fail-closed validation sequence.
	parsed, err := url.Parse(strings.TrimSpace(rawAddr))
	if err != nil ||
		!parsed.IsAbs() ||
		!strings.EqualFold(parsed.Scheme, "https") ||
		parsed.Host == "" ||
		parsed.User != nil ||
		parsed.RawQuery != "" ||
		parsed.ForceQuery ||
		parsed.Fragment != "" {
		return "", invalidVaultAddrIdentityError()
	}
	hostname, ipv6, err := canonicalVaultHostname(parsed.Hostname())
	if err != nil {
		return "", err
	}
	port, err := canonicalVaultPort(parsed.Host, parsed.Port())
	if err != nil {
		return "", err
	}
	authority := hostname
	if ipv6 {
		authority = "[" + strings.ReplaceAll(hostname, "%", "%25") + "]"
	}
	if port != "" {
		authority = net.JoinHostPort(hostname, port)
		if ipv6 {
			authority = strings.ReplaceAll(authority, "%", "%25")
		}
	}
	basePath := path.Clean(parsed.Path)
	if basePath == "." || basePath == "/" {
		basePath = ""
	}
	escapedPath := (&url.URL{Path: basePath}).EscapedPath()
	return "https://" + authority + escapedPath, nil
}

func canonicalVaultHostname(rawHostname string) (string, bool, error) {
	if rawHostname == "" {
		return "", false, invalidVaultAddrIdentityError()
	}
	if zoneIndex := strings.LastIndexByte(rawHostname, '%'); zoneIndex >= 0 {
		address := rawHostname[:zoneIndex]
		zone := rawHostname[zoneIndex+1:]
		ip := net.ParseIP(address)
		if ip == nil || ip.To4() != nil || zone == "" {
			return "", false, invalidVaultAddrIdentityError()
		}
		return strings.ToLower(ip.String()) + "%" + zone, true, nil
	}
	if ip := net.ParseIP(rawHostname); ip != nil {
		canonical := strings.ToLower(ip.String())
		return canonical, strings.Contains(canonical, ":"), nil
	}
	if strings.IndexFunc(rawHostname, func(char rune) bool {
		return char > unicode.MaxASCII || unicode.IsControl(char) || unicode.IsSpace(char)
	}) >= 0 || strings.ContainsAny(rawHostname, ":%[]") {
		return "", false, invalidVaultAddrIdentityError()
	}
	hostname := strings.ToLower(rawHostname)
	hostname = strings.TrimSuffix(hostname, ".")
	if hostname == "" || strings.HasSuffix(hostname, ".") || strings.Contains(hostname, "..") {
		return "", false, invalidVaultAddrIdentityError()
	}
	return hostname, false, nil
}

func canonicalVaultPort(rawHost, rawPort string) (string, error) {
	if rawPort == "" {
		if strings.HasSuffix(rawHost, ":") {
			return "", invalidVaultAddrIdentityError()
		}
		return "", nil
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return "", invalidVaultAddrIdentityError()
	}
	if port == 443 {
		return "", nil
	}
	return strconv.Itoa(port), nil
}

func invalidVaultAddrIdentityError() error {
	return apperrors.New(
		apperrors.CodeValidationFailed,
		"vaultAddr cannot be normalized to a safe HTTPS endpoint identity",
		nil,
	)
}

func validateContextImportCredentialCandidates(candidates []contextImportCredentialCandidate) error {
	planned := make(map[credentialPhysicalSlot]string)
	for _, candidate := range candidates {
		if candidate.backend == nil || !candidate.hasWrites() {
			continue
		}
		if err := candidate.backend.Available(); err != nil {
			return apperrors.New(
				apperrors.CodeCredentialStoreError,
				fmt.Sprintf("credential backend for context %q is unavailable", candidate.name),
				err,
			)
		}
		for _, slot := range candidate.slots() {
			if previous, exists := planned[slot]; exists {
				return apperrors.New(
					apperrors.CodeValidationFailed,
					fmt.Sprintf("credential import physical slot collision between contexts %q and %q", previous, candidate.name),
					nil,
				)
			}
			planned[slot] = candidate.name
		}
	}
	return nil
}

func validateContextImportCredentialKeySet(
	cfg *corectx.Config[mqgovctx.Context],
	expected map[string]mqgovctx.Context,
) error {
	contexts := make(map[string]mqgovctx.Context, len(cfg.Contexts)+len(expected))
	for name, item := range cfg.Contexts {
		contexts[name] = item
	}
	for name, item := range expected {
		contexts[name] = item
	}
	_, err := contextCredentialSlotOwners(contexts)
	return err
}

func contextCredentialSlotOwners(contexts map[string]mqgovctx.Context) (map[credentialPhysicalSlot]string, error) {
	owners := make(map[credentialPhysicalSlot]string)
	for name, item := range contexts {
		if ref := credstore.ParseRef(item.Password); ref.IsRef {
			slot, err := contextCredentialPhysicalSlot(item, ref.BackendName, name)
			if err != nil {
				return nil, err
			}
			if err := addContextImportCredentialOwner(owners, slot, name+"\x00primary"); err != nil {
				return nil, err
			}
		}
		if ref := credstore.ParseRef(item.KafkaSchemaRegistryPassword); ref.IsRef {
			slot, err := contextCredentialPhysicalSlot(item, ref.BackendName, name+"/schema-registry")
			if err != nil {
				return nil, err
			}
			if err := addContextImportCredentialOwner(
				owners,
				slot,
				name+"\x00schema-registry",
			); err != nil {
				return nil, err
			}
		}
	}
	return owners, nil
}

func addContextImportCredentialOwner(
	owners map[credentialPhysicalSlot]string,
	slot credentialPhysicalSlot,
	owner string,
) error {
	if previous, exists := owners[slot]; exists && previous != owner {
		return apperrors.New(
			apperrors.CodeValidationFailed,
			"credential physical slot collides with another context credential slot",
			nil,
		)
	}
	owners[slot] = owner
	return nil
}

func (candidate contextImportCredentialCandidate) hasWrites() bool {
	return candidate.password != "" || candidate.schemaRegistryPassword != ""
}

func (candidate contextImportCredentialCandidate) slots() []credentialPhysicalSlot {
	slots := make([]credentialPhysicalSlot, 0, 2)
	if candidate.password != "" {
		slots = append(slots, candidate.primarySlot)
	}
	if candidate.schemaRegistryPassword != "" {
		slots = append(slots, candidate.schemaRegistrySlot)
	}
	return slots
}

func storeContextImportCredentials(
	ctx context.Context,
	candidates []contextImportCredentialCandidate,
) (*contextImportCredentialTransaction, error) {
	transaction := &contextImportCredentialTransaction{}
	for _, candidate := range candidates {
		if candidate.password != "" {
			if err := transaction.put(
				ctx,
				candidate.backend,
				candidate.name,
				candidate.primarySlot,
				candidate.name+"\x00primary",
				candidate.password,
			); err != nil {
				return transaction, apperrors.New(
					apperrors.CodeCredentialStoreError,
					fmt.Sprintf("store credential for context %q failed", candidate.name),
					err,
				)
			}
		}
		if candidate.schemaRegistryPassword != "" {
			if err := transaction.put(
				ctx,
				candidate.backend,
				candidate.name+"/schema-registry",
				candidate.schemaRegistrySlot,
				candidate.name+"\x00schema-registry",
				candidate.schemaRegistryPassword,
			); err != nil {
				return transaction, apperrors.New(
					apperrors.CodeCredentialStoreError,
					fmt.Sprintf("store schema registry credential for context %q failed", candidate.name),
					err,
				)
			}
		}
	}
	return transaction, nil
}

func (transaction *contextImportCredentialTransaction) put(
	ctx context.Context,
	backend credstore.Backend,
	key string,
	slot credentialPhysicalSlot,
	owner string,
	value string,
) error {
	if backend == nil {
		return apperrors.New(apperrors.CodeCredentialStoreError, "credential backend is required", nil)
	}
	previous, err := backend.Get(ctx, key)
	existed := err == nil
	if err != nil && !errors.Is(err, credstore.ErrNotFound) {
		return err
	}
	transaction.writes = append(transaction.writes, contextImportCredentialWrite{
		backend:  backend,
		key:      key,
		slot:     slot,
		owner:    owner,
		previous: previous,
		written:  value,
		existed:  existed,
	})
	writeIndex := len(transaction.writes) - 1
	if err := backend.Put(ctx, key, value); err != nil {
		return err
	}
	transaction.writes[writeIndex].putSucceeded = true
	return nil
}

func (transaction *contextImportCredentialTransaction) compensationPlan(ctx context.Context) ([]bool, error) {
	if transaction == nil {
		return nil, nil
	}
	restore := make([]bool, len(transaction.writes))
	for index, write := range transaction.writes {
		needsRestore, err := credentialWriteNeedsRestore(
			ctx,
			write.backend,
			write.key,
			write.previous,
			write.written,
			write.existed,
			write.putSucceeded,
		)
		if err != nil {
			return nil, err
		}
		restore[index] = needsRestore
	}
	return restore, nil
}

func (transaction *contextImportCredentialTransaction) compensate(ctx context.Context, restore []bool) error {
	if transaction == nil {
		return nil
	}
	var compensationErr error
	for index := len(transaction.writes) - 1; index >= 0; index-- {
		if index >= len(restore) || !restore[index] {
			continue
		}
		write := transaction.writes[index]
		var err error
		if write.existed {
			err = write.backend.Put(ctx, write.key, write.previous)
		} else {
			err = write.backend.Delete(ctx, write.key)
		}
		if err != nil && compensationErr == nil {
			compensationErr = err
		}
	}
	return compensationErr
}

func compensateContextImportCredentialsLocked(
	ctx context.Context,
	cfg *corectx.Config[mqgovctx.Context],
	transaction *contextImportCredentialTransaction,
) (string, error) {
	if transaction == nil || len(transaction.writes) == 0 {
		return "", nil
	}
	if err := validateContextImportCompensationOwners(cfg, transaction); err != nil {
		return credentialCompensationNotSafe, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	compensationContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		credentialCompensationLimit,
	)
	defer cancel()
	restore, err := transaction.compensationPlan(compensationContext)
	if err != nil {
		return credentialCompensationNotSafe, err
	}
	if err := transaction.validateCompensationCapabilities(restore); err != nil {
		return credentialCompensationNotSafe, err
	}
	if err := transaction.compensate(compensationContext, restore); err != nil {
		return credentialCompensationIncomplete, err
	}
	return credentialCompensationSucceeded, nil
}

func (transaction *contextImportCredentialTransaction) validateCompensationCapabilities(restore []bool) error {
	for index, write := range transaction.writes {
		if index < len(restore) && restore[index] && write.slot.backendName == "vault" {
			return apperrors.New(
				apperrors.CodeCredentialStoreError,
				"Vault credentials cannot be compensated safely without compare-and-swap; refusing to overwrite",
				nil,
			)
		}
	}
	return nil
}

func validateContextImportCompensationOwners(
	cfg *corectx.Config[mqgovctx.Context],
	transaction *contextImportCredentialTransaction,
) error {
	if cfg == nil {
		return apperrors.New(apperrors.CodeCredentialStoreError, "context state is unavailable for credential compensation", nil)
	}
	owners, err := contextCredentialSlotOwners(cfg.Contexts)
	if err != nil {
		return err
	}
	for _, write := range transaction.writes {
		if owner, exists := owners[write.slot]; exists && owner != write.owner {
			return apperrors.New(
				apperrors.CodeCredentialStoreError,
				"credential slot ownership changed before compensation; refusing to overwrite",
				nil,
			)
		}
	}
	return nil
}

func credentialWriteNeedsRestore(
	ctx context.Context,
	backend credstore.Backend,
	key string,
	previous string,
	written string,
	existed bool,
	putSucceeded bool,
) (bool, error) {
	if backend == nil {
		return false, apperrors.New(apperrors.CodeCredentialStoreError, "credential backend is unavailable during compensation", nil)
	}
	current, err := backend.Get(ctx, key)
	currentExists := err == nil
	if err != nil && !errors.Is(err, credstore.ErrNotFound) {
		return false, err
	}
	if currentExists && current == written {
		return true, nil
	}
	if !putSucceeded && currentExists == existed && (!currentExists || current == previous) {
		return false, nil
	}
	return false, apperrors.New(
		apperrors.CodeCredentialStoreError,
		"credential value changed before compensation; refusing to overwrite",
		nil,
	)
}

func captureContextImportTargets(
	cfg *corectx.Config[mqgovctx.Context],
	names []string,
) map[string]contextImportTargetState {
	targets := make(map[string]contextImportTargetState, len(names))
	for _, name := range names {
		item, exists := cfg.Contexts[name]
		targets[name] = contextImportTargetState{context: item, exists: exists}
	}
	return targets
}

func contextImportConfigState(
	cfg *corectx.Config[mqgovctx.Context],
	original map[string]contextImportTargetState,
	expected map[string]mqgovctx.Context,
) (committed bool, unchanged bool) {
	committed = true
	unchanged = true
	for name, expectedItem := range expected {
		current, exists := cfg.Contexts[name]
		committed = committed && exists && contextImportItemsEqual(current, expectedItem)
		before := original[name]
		unchanged = unchanged &&
			exists == before.exists &&
			(!exists || contextImportItemsEqual(current, before.context))
	}
	return committed, unchanged
}

func contextImportItemsEqual(left, right mqgovctx.Context) bool {
	normalize := func(item mqgovctx.Context) mqgovctx.Context {
		if item.OTLPEndpointSource == "" {
			item.OTLPEndpointSource = "auto"
		}
		if item.OTLPMetricsSource == "" {
			item.OTLPMetricsSource = "auto"
		}
		if len(item.Roles) == 0 {
			item.Roles = nil
		}
		if len(item.Topics) == 0 {
			item.Topics = nil
		}
		if len(item.KafkaBrokers) == 0 {
			item.KafkaBrokers = nil
		}
		if len(item.RocketMQNameServers) == 0 {
			item.RocketMQNameServers = nil
		}
		return item
	}
	return reflect.DeepEqual(normalize(left), normalize(right))
}

func reconcileContextImportFailure(
	ctx context.Context,
	transaction *contextImportCredentialTransaction,
	original map[string]contextImportTargetState,
	expected map[string]mqgovctx.Context,
) (string, bool, error) {
	hasCredentialWrites := transaction != nil && len(transaction.writes) > 0
	committed := false
	unchanged := false
	compensationStatus := ""
	stateErr := withContextStoreLock(func(locked *corectx.Config[mqgovctx.Context]) error {
		committed, unchanged = contextImportConfigState(locked, original, expected)
		if committed {
			compensationStatus = credentialCompensationNotSafe
			return nil
		}
		if !unchanged {
			compensationStatus = credentialCompensationNotSafe
			return contextImportCompensationError(nil)
		}
		if !hasCredentialWrites {
			return nil
		}
		var err error
		compensationStatus, err = compensateContextImportCredentialsLocked(ctx, locked, transaction)
		return err
	})
	if stateErr != nil {
		if compensationStatus == "" {
			compensationStatus = credentialCompensationNotSafe
		}
		return compensationStatus, compensationStatus != credentialCompensationSucceeded, contextImportCompensationError(stateErr)
	}
	if committed {
		return credentialCompensationNotSafe, false, nil
	}
	if !hasCredentialWrites {
		return "", false, nil
	}
	return compensationStatus, false, nil
}

func contextImportCompensationError(cause error) error {
	return apperrors.New(
		apperrors.CodeCredentialStoreError,
		"context import failed and credential compensation could not be completed safely",
		cause,
	)
}

func finishContextImportAudit(
	handle *mutationAuditHandle,
	total int,
	compensationStatus string,
	compensationUncertain bool,
	operationErr error,
) error {
	outcome := mutationAuditOutcome{counted: true, CompensationStatus: compensationStatus}
	switch {
	case operationErr == nil:
		outcome.Status = audit.StatusSuccess
		outcome.Succeeded = total
	case compensationUncertain:
		outcome.Status = audit.StatusFailed
		outcome.Uncertain = total
	case compensationStatus == credentialCompensationNotSafe:
		outcome.Status = audit.StatusPartialFailed
		outcome.Succeeded = total
	case compensationStatus == credentialCompensationIncomplete:
		outcome.Status = audit.StatusFailed
		outcome.Uncertain = total
	default:
		outcome.Status = audit.StatusFailed
		outcome.Failed = 1
		outcome.Skipped = total - 1
		if outcome.Skipped < 0 {
			outcome.Skipped = 0
		}
	}
	return finishMutationAudit(handle, outcome, operationErr)
}
