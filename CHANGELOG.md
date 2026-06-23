# Changelog

All notable changes to this project are documented in this file.

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
