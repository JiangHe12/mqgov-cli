# Changelog

All notable changes to this project are documented in this file.

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
