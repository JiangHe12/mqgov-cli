# Changelog

All notable changes to this project are documented in this file.

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
