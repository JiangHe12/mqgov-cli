# mqgov-cli Agent Guide

This file is the contributor and AI-agent guide for this repository.
`CLAUDE.md` and `AGENTS.md` are kept identical; edit both together.
The workspace `../CLAUDE.md` (opskit family guide) and the global
`~/.claude/CLAUDE.md` rules also apply and take precedence.

## Project Summary

mqgov-cli is the governed message-broker operations CLI for AI agents: one entry
point for **Kafka**, **RabbitMQ**, **Pulsar**, and **RocketMQ**. It provides
backend-bound contexts, a fail-closed message-operation risk classifier
(`mqclass`), R0-R3 authorization with protected-context escalation,
native broker ACL management where the backend supports it, non-destructive peek/tail and bounded cross-context mirror where the source backend can guarantee it, dead-letter-queue governance (list/peek/redrive/purge) over each broker's native DLQ model, governed schema-registry inspection and mutation (list/describe/check/register/delete), a read-only cross-broker `fleet` view that aggregates R0 reads across contexts, real per-partition blast-radius previews, tamper-evident
fingerprint-only audit, and redaction. It is built on the shared `opskit-core`
governance engine.

## Working Discipline (how to work in this repo)

- Implement the task's goal within its stated boundaries. Do not invent scope,
  features, abstractions, or "future-proofing" nobody asked for.
- Make the smallest change that solves it; match surrounding style; remove any
  new unused imports/vars/flags you introduce.
- Never weaken governance, security, authorization, redaction, or audit to make
  code or a test pass. If a test seems to require that, the test is wrong.
- Do not modify `opskit-core`; consume its published APIs.
- Do not add design/spec/plan docs to the repo — change history lives in git.
- A change is complete only after ALL Build & Verify gates pass. Report the real
  results; never claim "should pass".

## Build & Verify (every gate must be green before "done")

```bash
go build ./...
CGO_ENABLED=0 go build ./...            # all broker clients must stay cgo-free
go test -count=1 ./...
gofmt -l main.go cmd internal           # must print nothing
golangci-lint run --timeout=5m
go vet -tags=integration ./...          # integration-tagged files are skipped otherwise
CGO_ENABLED=1 go test -race -count=1 ./cmd/ ./internal/mqclass/
go mod tidy                             # must be a no-op
```

- Real-backend integration tests (`//go:build integration`, env-gated, skipped by
  default) cover Kafka/RabbitMQ/Pulsar/RocketMQ; they run in the nightly
  `integration.yml` workflow against the bundled `docker-compose.*.yml`, not on push/PR.
- A passing env-gated integration test that SKIPs (endpoint env unset) is reported
  as "ok" — it never ran. Dogfood the real backend yourself; a green default test
  suite does not prove the backend works.
- README / SKILL.md command examples are NOT covered by CI: run the real binary
  and confirm every cited flag exists (`mqgov <cmd> --help`) before shipping docs.

## Governance Rules (non-negotiable)

- R0 reads are free but audited. R1 needs `--yes`. R2 also needs a non-empty
  `--ticket`. R3 also needs the exact command-specific `--allow-*` flag
  (`--allow-offset-reset`, `--allow-topic-purge`, `--allow-topic-delete`,
  `--allow-destructive-acl`, `--allow-internal-produce`,
  `--allow-schema-delete`).
- Protected contexts, protected topics, and internal/system topics raise every
  operation one tier; authorization must go through `opskit-core/safety`
  (`EffectiveRisk` + `Authorize`).
- `mqclass` is the only message-operation risk source and must stay fail-closed
  and structure-aware: offset reset/seek, purge, topic delete, schema delete,
  ACL revoke, broad ACL grants, produce/mirror-to-internal are pinned R3;
  wildcard/glob targets escalate; unknown/uncertain inputs escalate, never fall
  to R0. Schema register is R1 for a new subject and R2 for an existing or
  uncertain subject. Mirror source authorization uses read semantics on the
  source context; target authorization follows produce semantics on the target
  context.
- `--dry-run`/`--plan` is a read-only (R0) impact preview that must NEVER mutate.
  In a command, the SAME `dryRun` flag must drive both the R0 classification and
  the non-mutating execution path — they cannot be decoupled (no "R0-auth-but-mutate").
- AI agents never auto-fill `--ticket`, `--allow-*`, or a high-risk `--yes`.
  Blast radius comes from `--dry-run`/`--plan`, never a model guess.
- Audit stores only metadata, sha256 fingerprints, and counts — never raw message
  bodies, keys, headers, tickets, or reasons. `Message.Key/Body/Headers` are
  `json:"-"` so payloads can never serialize. Redaction applies before caller
  output and before audit persistence.
- Peek/tail must be non-destructive (no consume, no cursor/offset advance) or
  fail closed (`NotImplemented`) — never silently consume on an R0 read.
- No insecure transport: SASL/TLS and mTLS via credstore; never an
  insecure-skip-verify option. A backend that does not support requested TLS
  fails closed; it never silently connects in plaintext.

## Backends — the dumb-adapter contract

- Backends implement `mqgov.Broker`; optional capabilities use the separate
  `OffsetManager` / `PartitionManager` / `ACLManager` / `Tailer` / `DLQManager` / `SchemaManager` interfaces, type-asserted
  and gated by `Supports*`. Unsupported capabilities fail closed with
  `NotImplemented` — never faked. Capabilities must reflect what the client
  actually supports (e.g. RabbitMQ/RocketMQ `SupportsOffsets=false`; RabbitMQ/RocketMQ do not support non-destructive tail or mirror source; Kafka and Pulsar support mirror source through non-destructive reads; Kafka, RabbitMQ, and Pulsar support ACL, RocketMQ does not; Kafka has no native DLQ discovery so DLQ list is NotImplemented while peek/redrive/purge work on an explicit DLQ topic, RocketMQ supports only DLQ list; Kafka and Pulsar support schema register/delete through native schema registries, RabbitMQ and RocketMQ do not).
- **Backends are dumb**: they only execute broker operations. All R0-R3
  authorization stays in `cmd/` + `mqclass`; a backend must never make an
  authorization decision.
- ACL mappings must stay backend-native and honest: Kafka uses broker ACLs with
  literal/prefixed patterns; RabbitMQ uses user-vhost permission regexes
  (`configure`, `write`, `read`) and allow-only grants; Pulsar uses namespace/topic
  role permissions (`produce`, `consume`, `functions`, `sources`, `sinks`,
  `packages`) and allow-only grants. Do not emulate unsupported
  ACL models or silently translate deny into allow.
- All broker clients must be cgo-free (Kafka franz-go, RabbitMQ amqp091-go,
  Pulsar pulsar-client-go, RocketMQ rocketmq-client-go/v2). Never the legacy/cgo
  variants — they break the multi-platform release matrix.
- Backend-specific addressing (Kafka topic/partition; RabbitMQ exchange/queue +
  management API; Pulsar persistent://tenant/ns/topic + admin REST; RocketMQ
  topic/nameserver/broker-addr) stays inside the adapter.

## Code Conventions

- `cmd/` uses `apperrors.New`; bare `fmt.Errorf`/`errors.New` are forbidden there
  (forbidigo CI guard) and exit codes come from the `apperrors` contract.
- Reuse opskit-core for contexts, credentials, safety, audit, printing,
  redaction, telemetry, errors, and lockfile — never reimplement them.
- Wrap backend errors with the right code (`CodeBackendError`/`CodeBackendUnreachable`/
  `CodeResourceNotFound`/`CodeResourceAlreadyExists`) and preserve the cause;
  never leak message content into error messages.
- Add focused table-driven and adversarial tests for security-sensitive changes;
  do not weaken production behavior for tests.
- Keep `.gitattributes` (`eol=lf`) so the Windows lint job does not fail gofmt on
  a CRLF checkout.

## Repository Layout

- `cmd/` - Cobra commands (`topic`/`group`/`message`/`dlq`/`acl`/`schema`/`fleet`/`ctx`/`audit`/`install`/…) and `-o json` output contracts
- `internal/backend/{kafka,rabbitmq,pulsar,rocketmq,fake}` - broker adapters
- `internal/mqgov` - Broker abstraction + coordinate/fingerprint types + optional capability interfaces
- `internal/mqclass` - fail-closed message-operation risk classifier
- `internal/mqgovctx` - backend-bound contexts + credential resolution
- `skills/mqgov-cli/` - embedded AI Skill (keep in sync with the real flags)
- `bin/` · `scripts/` · `.github/workflows/` - npm shim, installer, CI/release
- `docker-compose.*.yml` · `testdata/` · `docs/` - local real-backend setups + integration docs

## Release & Versioning (maintainer-owned — do not initiate)

Releases are cut by the maintainer only; do not tag, publish, or edit artifacts.

**Docs-before-release gate (mandatory).** A release ships only after every
user-facing doc already matches the code's actual state — `README.md`,
`README_zh.md`, `skills/mqgov-cli/SKILL.md`, this guide (`CLAUDE.md`/`AGENTS.md`),
and the `package.json` description. Any new backend, noun/verb, flag, risk tier,
or dependency / Go-version bump must be reflected first (confirm examples with
`mqgov <cmd> --help`). Code must never ship ahead of its docs.

For reference, a release bumps `package.json`, adds an exact `## vX.Y.Z`
`CHANGELOG.md` heading, passes Build & Verify (`npm pack --dry-run` lists exactly
`LICENSE`, `README.md`, `package.json`, `bin/mqgov-cli.js`, `scripts/install.js`),
then pushes tag `vX.Y.Z`. **npm publish is locked to the CI trusted publisher via
OIDC; local/token `npm publish` is disabled — never attempt a manual publish.**
