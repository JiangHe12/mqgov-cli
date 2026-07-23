# Changelog

All notable changes to this project are documented in this file.

## v0.6.3

### Security

- All broker reads and mutation preflights now persist a read intent before
  authorization and client construction, and persist the correlated outcome
  before releasing output or beginning mutation authorization and execution.
  Audit failures fail closed without backend access, result release, or writes.
- Backend diagnostics are redacted, bounded to 64 KiB, and deferred until the
  read outcome is durable; third-party client logging that cannot participate
  in this lifecycle remains disabled.
- Message mirror source staging is capped by the exact requested limit of
  1–1,000 messages and a conservative 64 MiB accounting budget, and the source
  audit outcome must be durable before any target mutation.
- npm installation now trusts only the exact six platform digests embedded in
  the provenance-bound package manifest. Release automation verifies the signed
  checksum and binary bundle before GitHub Release and npm publication, while
  verified downloads use exclusive temporary files, bounded transfers, fsync,
  and atomic replacement without a verification bypass.

### Changed

- **Fail-closed compatibility change:** RabbitMQ message and DLQ peek now return
  `NOT_IMPLEMENTED` because consume-and-requeue cannot prove a truly
  non-consuming read.
- Peek and tail limits are bounded to 1–10,000 messages, and mirror limits are
  bounded to 1–1,000 messages at both command and backend boundaries.
- Updated `opskit-core/v2` to v2.0.2 for shared owner-only, no-follow,
  durable-atomic context, encrypted credential, and TLS trust-pin storage.
- Updated `golang.org/x/crypto`, `x/net`, `x/sys`, and `x/term` to their current
  patched releases.

### Fixed

- Backend, cluster, and topic coordinates are normalized and bound consistently
  across authorization and execution, including exact topic classification and
  fail-closed metadata drift checks.
- Batch operations now report bounded succeeded, failed, and uncertain counts
  instead of treating partial backend results as complete success.
- Invalid output formats are rejected before command execution.
- Updated Cosign to v2.6.4 and hardened CI checks for installer tests and the
  exact five-file npm package.

## v0.6.2

### Security

- Upgraded `golang.org/x/text` to `v0.39.0`, removing the dependency version affected by `GO-2026-5970`; no reachable vulnerable symbol was found in mqgov-cli.

### Fixed

- Release checksum aggregation now merges matrix artifacts without Unix binary/directory name collisions, verifies all six per-platform checksum files, and fails unless the global manifest contains exactly six binaries. The v0.6.1 per-platform checksums and Cosign signatures remain valid, but its global manifest omitted the four Unix binaries.

## v0.6.1

### Changed

- Pinned the Kafka/Schema Registry, RabbitMQ, Pulsar, and RocketMQ integration images to the exact digests exercised by the v0.6.0 tag integration run.
- Made the complete real-backend matrix (Kafka, ACL, TLS pinning, RabbitMQ, Pulsar, RocketMQ, and Kafka-to-Pulsar mirror) a reusable release gate with required mode preventing missing endpoints from becoming green skips.
- Added a release preflight that requires a GitHub-verifiable signed annotated tag matching `package.json`, an exact literal `CHANGELOG.md` heading, and the freshly fetched `origin/main` commit; release jobs rerun the complete CI/security gate on that exact tag commit.
- **Fail-closed compatibility change:** RocketMQ topic delete is now `NOT_IMPLEMENTED` because the upstream v2 admin client ignores broker/name-server response codes and route disappearance cannot prove broker-side deletion. RocketMQ namespace configuration is also rejected because the client applies namespace wrapping inconsistently across admin operations.

### Fixed

- Recognized Pulsar's official `compatibility` and `isCompatibility` response fields while keeping missing, malformed, conflicting, and conflicting duplicate results fail-closed.
- Classifies RocketMQ topic creation as R2 because the upstream API is an upsert, checks every configured name server for absence before dispatch, and confirms the actual route and requested queue count separately through every name server after creation. Once the create RPC is entered, any request, queue-count conflict, or confirmation error is reported as `PARTIAL_FAILURE` with an uncertain audit outcome and a non-zero CLI exit instead of implying that no side effect occurred; the upstream client's fixed in-call route/write timeouts remain documented.

## v0.6.0

### Added

- Added two-phase mutation auditing with intent-before-effect, correlated outcomes, and commit-aware recovery of definitely uncommitted audit outcomes.

### Changed

- **BREAKING**: Context and role changes plus confirmed audit pruning are fixed R3 governance operations requiring their precise `--allow-*` flags.
- Updated to `opskit-core/v2` v2.0.0. Confirmed audit pruning now validates authenticated history, advances checkpoints safely, and records its intent and outcome in a sibling control log.

### Fixed

- Made broker capability reporting and destructive behavior fail closed: RabbitMQ redrive uses publisher confirms and removes only acknowledged source messages, Kafka refuses non-atomic DLQ redrive, and Pulsar refuses unsupported redrive, purge, and non-measurable offset resets.
- RabbitMQ peek now restores the complete unacknowledged batch, Pulsar latest reset derives impact from live partition backlog, TLS mode requires secure endpoint schemes, and broker clients now release their transport resources.
- Context import validates all targets before writing and compensates rollback-safe credential changes on config-commit failure. Vault slots are matched by address, namespace, and path, collisions are rejected, and rollback fails closed without atomic compare-and-swap; file exports use bounded atomic replacement and reject reserved-path aliases.

### Security

- Authorization identity now derives from the local OS user and hostname. Mutation audit and replay storage redact payloads and secrets, queue only definitely uncommitted outcomes, and quarantine indeterminate replay.

## v0.5.10

### Changed

- Updated opskit-core to v1.1.4.

## v0.5.9

### Changed
- CLI-owned environment variables now prefer the family-standard `MQGOV_*` names: `MQGOV_AUDIT_PRIVATE_KEY`, `MQGOV_CREDENTIAL_PASSPHRASE`, `MQGOV_OPERATOR`, `MQGOV_DOWNLOAD_MIRROR`, and `MQGOV_SKIP_VERIFY`. Deprecated `MQGOV_CLI_*` aliases remain supported for compatibility.

## v0.5.8

### Changed
- **BREAKING**: `capabilities -o json` schema was restructured for family alignment; domain-specific fields moved to `data.domain`.

## v0.5.7

### Added
- Global flags: `--debug`, `--trace`, `--no-color`.

## v0.5.6

### Added
- `ctx export`, `ctx import`, `ctx role`, and `ctx migrate-credentials` subcommands.
- `audit prune` subcommand for rotated audit log cleanup.

## v0.5.5

### Changed
- Simplified `version -o plain` and `capabilities -o plain` output to the script-friendly family format.

## v0.5.4

### Changed
- Fixed release version injection by bridging main package build variables into `cmd`.
- Aligned root `--version` output with the family format by using the full CLI name.

## v0.5.3

### Added
- Operation commands now show their target context/backend/cluster/namespace in table/plain output and JSON data.target.

## v0.5.2

### Changed
- Reuse opskit-core's shared secure-backend guard for stored credentials; behavior is unchanged.

## v0.5.1

_RabbitMQ credential-supply fixes._

### Added
- `ctx set --backend rabbitmq --username` to set the RabbitMQ user with the
  discrete `--host`/`--port`/`--vhost` flags, instead of being forced to embed it
  in `--amqp-url`.

### Fixed
- `MQGOV_PASSWORD` is now honored at connection time when a context has no stored
  credential (current context and `--context` overrides), giving a consistent
  non-interactive password path. RabbitMQ AMQP and management API now use one
  unified username/password: explicit `--username`/password (or `MQGOV_PASSWORD`)
  overrides `--amqp-url` userinfo, with `guest/guest` as the default.

## v0.5.0

_Broker and schema-registry TLS certificate trust-on-first-use pinning._

### Added

- Added TLS certificate trust-on-first-use (TOFU) pinning for Kafka, RabbitMQ,
  and Pulsar connections (broker and HTTP/admin/schema-registry surfaces). On the
  first TLS connection the server leaf certificate's SPKI-SHA256 is pinned in
  `.mqgov-cli/tls_known_hosts`; any later SPKI mismatch hard-fails the connection
  with `AUTH_FAILED`. Pinning runs on top of normal certificate-chain
  verification (never `InsecureSkipVerify`) and is default-on whenever a
  connection uses TLS. RocketMQ has no TLS client path to pin.

### Changed

- Bumped `opskit-core` to v1.1.0 for the shared `trust` pin store.

## v0.4.0

_Governed schema-registry mutation and bounded cross-broker message mirroring._

### Added

- `schema register|delete` — governed schema-registry write operations. `register`
  is R1 for a new subject and R2 for an existing or uncertain subject (an existing
  subject is compatibility-checked first and an incompatible schema is refused);
  `delete` is R3 behind the command-specific `--allow-schema-delete`. **Kafka** maps
  Confluent Schema Registry register plus soft/`--permanent` delete; **Pulsar**
  supports permanent subject delete only and returns `NOT_IMPLEMENTED` for soft or
  per-version delete; RabbitMQ/RocketMQ stay fail-closed.
- `message mirror SOURCE_TOPIC --to-context NAME --to-topic NAME --limit N` — a
  bounded one-shot copy of messages from one context's topic to another, across
  brokers. Source positions: `earliest|latest|offset:N|timestamp:<RFC3339>` plus
  `--partition` for **Kafka**; `earliest|latest|timestamp` for **Pulsar** (`offset:N`
  and partition-specific mirror are refused). RabbitMQ/RocketMQ source is fail-closed
  `NOT_IMPLEMENTED` (read-is-consume / offset-committing). There is no daemon/follow mode.

### Governance

- `schema register`/`delete` and `message mirror` go through the same fail-closed
  `mqclass` classifier. `message mirror` is dual-authorized: the source read is
  classified R0 against the source context (a protected source escalates the read and
  blocks cheap exfiltration), and the target write is classified as a produce against
  the target context (protected target → R2, internal/system target → R3 via
  `--allow-internal-produce`); a wildcard target is rejected.
- `--dry-run`/`--plan` for `message mirror` stays R0 and never produces — the same
  `dryRun` flag drives both classification and the no-write path, and the message
  count is computed by the CLI.

### Security

- `message mirror` audit records only source/target coordinates, a message count, and
  an aggregate sha256 of the bodies — never message keys, bodies, or headers, which
  flow only in memory. Schema register/delete audit records subject/version and a
  sha256 of the schema text, never the schema body.

## v0.3.0

_Dead-letter-queue governance, read-only schema-registry inspection, and a read-only cross-broker fleet view._

### Added

- `dlq list|peek|redrive|purge` — governed dead-letter-queue operations mapped to
  each broker's native DLQ model: **Kafka** (no native discovery — `list` is
  `NOT_IMPLEMENTED`; peek/redrive/purge on an explicitly named DLQ topic),
  **RabbitMQ** (dead-letter-exchange queues; redrive uses publisher confirms so a
  dead-letter is never lost to an unroutable target), **Pulsar**
  (`{topic}-{subscription}-DLQ`), **RocketMQ** (`list` of `%DLQ%{group}` only;
  peek/redrive/purge `NOT_IMPLEMENTED`).
- `schema list|describe|check` — read-only schema-registry inspection. **Kafka**
  via a Confluent Schema Registry (optional `--schema-registry-url` + credstore
  credentials on the context); **Pulsar** via the built-in schema admin API;
  RabbitMQ/RocketMQ `NOT_IMPLEMENTED`. `check` is a compatibility check only and
  never registers a schema.
- `fleet status|topics --all|--contexts a,b` — a read-only view that aggregates
  R0 reads across multiple contexts, tagged per context, with honest per-context
  status (`denied`/`unreachable`/`error`).

### Governance

- All new operations are read-only: `dlq list/peek`, `schema list/describe/check`,
  and `fleet status/topics` are R0; `dlq redrive` (real execution) is pinned R3 via
  `--allow-internal-produce` (it produces into a live topic) and `dlq purge` is R3
  via `--allow-topic-purge`; `--dry-run`/`--plan` previews stay R0 and never mutate.
- `fleet` is pure aggregation: each context is authorized independently through the
  same R0 classification as a single-context read, with its own credentials. There
  is no cross-broker write path.
- RocketMQ native ACL is documented and locked as fail-closed `NOT_IMPLEMENTED`:
  `rocketmq-client-go/v2` exposes no clean, cgo-free broker ACL admin API.

### Security

- Kafka Schema Registry basic-auth credentials require `https`; a configured
  username/password with a non-https URL fails closed and is never transmitted in
  plaintext. The SR password is stored through credstore, separate from the broker
  SASL credential. Audit records schema subject/version/compatibility and a sha256
  of the schema text — never the schema body or credentials.

## v0.2.0

_Continuous non-destructive tail, and native broker ACL management across Kafka, RabbitMQ, and Pulsar._

### Added

- `message tail` — a continuous, non-destructive, fingerprint-only message stream (the
  broker analogue of `tail -f`), bounded by `--max-messages`/`--timeout`/`--follow` and
  starting from `--from earliest|latest|offset:N`. Supported on **Kafka** (groupless
  direct-partition read) and **Pulsar** (Reader); RabbitMQ and RocketMQ fail closed with
  `NOT_IMPLEMENTED`.
- `acl list|grant|revoke` — governed broker ACL management, mapped to each backend's
  native authorization model:
  - **Kafka** — broker ACLs (principal, resource type/name, `literal`/`prefixed` pattern,
    operation, allow/deny) via kadm.
  - **RabbitMQ** — per-user, per-vhost permission regexes (`configure`/`write`/`read`),
    allow-only, via the management API (`--vhost`, `--pattern regex`).
  - **Pulsar** — namespace/topic role permissions (`produce`/`consume`/`functions`/
    `sources`/`sinks`/`packages`), allow-only, via the admin REST API.

  RocketMQ fails closed with `NOT_IMPLEMENTED`.

### Governance

- `message tail` is R0 (free, audited) and classified identically to `peek` under target
  escalation; it can never reach a destructive tier and never commits an offset or advances
  a cursor. A single aggregate, fingerprint-only audit event covers the whole stream.
- `mqclass` ACL classification is fail-closed and structure-aware: `acl list` is R0,
  `acl grant` is R2 escalating to R3 (`--allow-destructive-acl`) for broad grants — wildcard
  or empty principal/resource, cluster/`all`/`alter` operations, Kafka `prefixed` patterns,
  broad RabbitMQ regexes (`.*`, `.+`, `.`, `orders.*`), and Pulsar
  `functions`/`sources`/`sinks`/`packages` — and every `acl revoke` is R3.
- ACL mappings stay backend-native and honest: `deny` and unsupported pattern types are
  rejected rather than silently translated, and ACL support is reported per backend.

### Security

- New `--allow-destructive-acl` flag gates R3 ACL operations; AI callers must never
  self-fill it. ACL changes are audited with the full binding — never credentials.

## v0.1.0

_First public release. The governed message-broker operations CLI of the opskit family._

### Added

- Governed broker operations over four backends — **Kafka** (franz-go), **RabbitMQ**
  (amqp091-go + management API), **Pulsar** (pulsar-client-go + admin REST), and
  **RocketMQ** (rocketmq-client-go/v2) — behind one backend-agnostic governance spine.
- `topic` (list, describe, create, alter, delete, purge), `group`
  (list, create, delete, lag, reset-offset), and `message` (peek, produce) verbs.
- Backend-bound contexts via `ctx` (set/use/list/current/delete/test) with
  credentials stored through `opskit-core` credstore (never plaintext).
- Tamper-evident, fingerprint-only audit via `audit` (query, verify) — message
  bodies, keys, and headers are never persisted.
- Static commands: `version`, `capabilities`, `doctor`, `completion`, and
  `install <agent> --skills` for the embedded AI Skill.

### Governance

- R0–R3 risk model via the shared `opskit-core/safety` engine: R0 reads are free
  but audited, R1 needs `--yes`, R2 also needs `--ticket`, R3 also needs the exact
  `--allow-*` flag; protected contexts and protected/internal topics escalate one tier.
- Fail-closed, structure-aware `mqclass` classifier: offset reset/seek, purge,
  delete, and produce-to-internal-topic are pinned R3; wildcard/glob targets
  escalate; unknown operations fail closed to the highest tier.
- `--dry-run`/`--plan` impact previews are read-only (R0) and never mutate; AI
  callers must never self-fill `--ticket`, `--allow-*`, or a high-risk `--yes`.
- Non-destructive peek where the broker supports it (Pulsar Reader, RabbitMQ
  get+requeue); RocketMQ peek fails closed because its client cannot guarantee it.
- Capabilities are reported per backend (`SupportsOffsets`/`SupportsPartitions`/
  `SupportsACL`); unsupported operations fail closed with `NOT_IMPLEMENTED`.

### Security

- SASL/TLS and mTLS connections only, never insecure-skip-verify; credentials via
  credstore. Binaries are cosign-signed and the npm package ships with provenance;
  the installer verifies a SHA-256 checksum before use.
