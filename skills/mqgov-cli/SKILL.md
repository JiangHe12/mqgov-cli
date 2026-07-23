---
name: mqgov-cli
description: Governed message middleware operations across Kafka, RabbitMQ, Pulsar, and RocketMQ with R0-R3 authorization, context/credstore support, and fingerprint-only audit.
allowed-tools: Bash(mqgov:*), Bash(mqgov-cli:*)
---

# mqgov-cli

Use `mqgov` for governed message middleware operations. Prefer `-o json` for machine parsing.

Hard rules:

- Never invent or self-fill `--ticket`, `--yes`, or `--allow-*`; these are human authorization inputs.
- Authorization/audit identity is the local OS `username@hostname`; `--operator`
  and MQGOV operator environment variables are ignored. This does not separate
  an AI from a human under the same OS account.
- Message body, key, and headers must not be copied into audit summaries or tickets. Use fingerprints and counts.
- All broker access, including mutation preflights, is fail-closed audited: intent must be durable, then local R0/context-role authorization must pass, before any client is built or connected. The paired outcome with the same `operationId` must be durable before output or advanced mutation authorization/intent/write. Treat `LOCAL_IO_ERROR` as no-result/no-mutation; never retry past a broken audit store.
- Check `mqgov capabilities -o json` before assuming a backend supports offsets, partitions, ACL, DLQ verbs, peek, or tail.
- Unsupported backend operations fail closed with `NOT_IMPLEMENTED`.
- TLS for Kafka, RabbitMQ, and Pulsar pins the server leaf SPKI-SHA256 on first use in `.mqgov-cli/tls_known_hosts`; never suggest insecure-skip-verify or deleting pins without human certificate-rotation review.

Common setup:

```bash
mqgov ctx set dev --backend kafka --brokers localhost:9092 --cluster dev --yes --ticket <ticket> --allow-context-change
mqgov ctx set dev-sr --backend kafka --brokers localhost:9092 --schema-registry-url https://schema-registry.example --schema-registry-username <user> --schema-registry-password <password> --credential-backend encrypted-file --yes --ticket <ticket> --allow-context-change
mqgov ctx set dev-rabbit --backend rabbitmq --host localhost --port 5672 --vhost / --management-url http://localhost:15672 --username guest --yes --ticket <ticket> --allow-context-change
mqgov ctx set dev-pulsar --backend pulsar --service-url pulsar://localhost:6650 --admin-url http://localhost:8080 --tenant public --pulsar-namespace default --yes --ticket <ticket> --allow-context-change
mqgov ctx set dev-rocket --backend rocketmq --nameservers localhost:9876 --broker-addr localhost:10911 --yes --ticket <ticket> --allow-context-change
mqgov ctx use dev --yes --ticket <ticket> --allow-context-change
mqgov ctx export dev > dev.ctx.yaml
mqgov ctx import -f dev.ctx.yaml --yes --ticket <ticket> --allow-context-change
mqgov ctx role set dev --target-operator <operator> --role reader|writer|admin --yes --ticket <ticket> --allow-role-change
mqgov ctx migrate-credentials --dry-run
mqgov ctx migrate-credentials --yes --ticket <ticket> --allow-context-change
mqgov ctx test -o json
```

For RabbitMQ, provide `MQGOV_PASSWORD` when running commands if the context has no stored credential. To persist a password, use `--password` with `--credential-backend keychain` or `--credential-backend encrypted-file`. Explicit `--username` and password sources override userinfo embedded in `--amqp-url` and are used for both AMQP and management API auth.

Context import is validate-first across the full document. If secure credential writes succeed but the context config cannot commit, mqgov restores the previous credential values when it can prove compensation is safe; otherwise it fails closed and records an uncertain/incomplete compensation outcome.

Reads:

```bash
mqgov topic list -o json
mqgov topic describe <topic> -o json
mqgov group list -o json
mqgov group lag <group> <topic> -o json
mqgov message peek <topic> --partition 0 --offset 0 --count 1 -o json
mqgov message tail <topic> --from earliest --max-messages 10 --timeout 30s -o json
mqgov message mirror <source-topic> --to-context <target-context> --to-topic <target-topic> --limit 100 --dry-run -o json
mqgov dlq list --topic <topic> --group <group-or-sub> -o json
mqgov dlq peek <dlq> --count 1 -o json
mqgov acl list --principal User:svc -o json
mqgov schema list -o json
mqgov schema describe <subject-or-topic> --version latest -o json
mqgov schema check <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO -o json
mqgov fleet status --all -o json
mqgov fleet topics --contexts dev,staging -o json
```

Peek counts and tail `--max-messages` must be between 1 and 10,000. Results preserve broker read order, return at most the requested count, and report the actual shorter count at the current boundary. RabbitMQ message/DLQ peek returns `NOT_IMPLEMENTED` because consume/requeue cannot prove a truly non-consuming read. Tail uses a small initial buffer and releases no result before the read outcome is durable.

Writes require human authorization according to risk:

```bash
mqgov topic create <topic> --partitions 3 --yes -o json
mqgov topic create <topic> --partitions 3 --yes --ticket <ticket> -o json  # RocketMQ: upstream API is update-or-create (R2)
mqgov topic create <topic> --partitions 3 --yes --ticket <ticket> --allow-topic-upsert -o json  # protected RocketMQ (R3)
mqgov message produce <topic> --body <text> --yes -o json
mqgov group reset-offset <group> <topic> --to earliest --dry-run -o json
mqgov group reset-offset <group> <topic> --to latest --yes --ticket <ticket> --allow-offset-reset -o json
mqgov topic purge <topic> --dry-run -o json
mqgov topic purge <topic> --yes --ticket <ticket> --allow-topic-purge -o json
mqgov topic delete <topic> --yes --ticket <ticket> --allow-topic-delete -o json  # supported backends only; RocketMQ is NOT_IMPLEMENTED
mqgov message mirror <source-topic> --to-context <target-context> --to-topic <target-topic> --limit 100 --yes -o json
mqgov schema register <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO --yes -o json
mqgov schema register <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO --yes --ticket <ticket> -o json
mqgov schema delete <subject-or-topic> --yes --ticket <ticket> --allow-schema-delete -o json
mqgov schema delete <subject-or-topic> --version <version> --permanent --yes --ticket <ticket> --allow-schema-delete -o json
mqgov dlq redrive <dlq> --target <live-topic> --count 100 --dry-run -o json  # RabbitMQ
mqgov dlq redrive <dlq> --target <live-topic> --count 100 --yes --ticket <ticket> --allow-internal-produce -o json  # RabbitMQ
mqgov dlq purge <dlq> --dry-run -o json
mqgov dlq purge <dlq> --yes --ticket <ticket> --allow-topic-purge -o json
mqgov acl grant --principal User:svc --resource-type topic --resource-name <topic> --pattern literal --operation read --permission allow --yes --ticket <ticket> -o json
mqgov acl revoke --principal User:svc --resource-type topic --resource-name <topic> --pattern literal --operation read --permission allow --yes --ticket <ticket> --allow-destructive-acl -o json
mqgov acl grant --principal svc --vhost / --resource-type vhost --resource-name '^orders$' --pattern regex --operation read --permission allow --yes --ticket <ticket> -o json
mqgov acl revoke --principal svc --vhost / --resource-type vhost --resource-name '^orders$' --pattern regex --operation read --permission allow --yes --ticket <ticket> --allow-destructive-acl -o json
mqgov acl grant --principal app-role --resource-type namespace --resource-name public/default --pattern literal --operation produce --permission allow --yes --ticket <ticket> -o json
mqgov acl revoke --principal app-role --resource-type topic --resource-name <topic> --pattern literal --operation consume --permission allow --yes --ticket <ticket> --allow-destructive-acl -o json
```

RocketMQ governance: `topic create` is R2 because the upstream API is update-or-create; protected targets are R3 and require `--allow-topic-upsert`. The backend checks and then confirms the actual queue route through every configured name server, but in-flight upstream route calls have a fixed client timeout and cannot be interrupted immediately. Treat every `PARTIAL_FAILURE` as uncertain and reconcile before retrying. RocketMQ topic delete and `--namespace` are fail-closed `NOT_IMPLEMENTED` because the v2 admin client cannot prove broker-side deletion and applies namespace wrapping inconsistently.

ACL governance:

- Kafka, RabbitMQ, and Pulsar support `acl list|grant|revoke`; RocketMQ fails closed with `NOT_IMPLEMENTED` because `rocketmq-client-go/v2` exposes no public, cgo-free ACL admin API. Manage RocketMQ ACLs out of band with broker-side `plain_acl.yml` or the official Java `mqadmin` until the Go client exposes a clean API.
- Kafka uses broker ACLs with `literal`/`prefixed` patterns. RabbitMQ uses native user-vhost permission regexes with operations `configure`, `write`, and `read`; RabbitMQ rejects deny and non-regex patterns.
- Pulsar uses native role permissions on namespaces or topics with actions `produce`, `consume`, `functions`, `sources`, `sinks`, and `packages`; Pulsar rejects deny and non-literal patterns.
- `acl list` is R0. Normal `acl grant` is R2. Broad grants, including Kafka prefixed patterns, broad RabbitMQ regexes such as `.*`, `.+`, `.`, or `orders.*`, and Pulsar `functions`/`sources`/`sinks`/`packages`, and every `acl revoke` are R3 and require `--allow-destructive-acl`.

Message mirror governance:

- `message mirror SOURCE --to-context NAME --to-topic NAME --limit N` is a bounded one-shot copy only. Never use it as a daemon and never add `--follow`.
- Mirror `--limit` is 1–1,000. Source staging is bounded by both that exact request limit and a conservative 64 MiB accounting budget charging actual key/body/header bytes plus fixed per-message/per-header overhead; a backend emission past either boundary fails closed, wipes staged copies, and performs zero target writes.
- Mirror resolves each topic once, then uses two independent authorizations: source-side non-destructive read under the source context policy, then target-side produce under the persisted target context policy. Either failure precedes message reads and target writes. Protected source contexts can require approval before exfiltration; protected/internal targets follow produce risk and internal targets require `--allow-internal-produce`.
- Source intent precedes authorization/client construction and source outcome durability precedes target mutation intent/produce. `--dry-run` / `--plan` follows the same mandatory source audit pair and must not mutate the target.
- Kafka and Pulsar can be mirror sources. RabbitMQ and RocketMQ sources fail closed with `NOT_IMPLEMENTED` because their available reads cannot guarantee non-destructive full-message reads.
- Key/body/headers may flow only in process memory from source to target. Source and destination must be audited separately with their own context, target, request/result fingerprint, and count; never raw key, body, or headers.

Schema governance:

- Schema read verbs are `schema list|describe|check`; all are R0 and audited. `check` is a read-only compatibility check and must never register, delete, or evolve a schema.
- `schema register` is R1 for a new subject (`--yes`) and R2 for an existing subject (`--yes --ticket`). Registering to an existing subject is schema evolution; there is no separate evolve verb.
- Existing-subject register first runs the backend compatibility check. If incompatible, stop; do not register.
- `schema delete` is R3 and requires `--yes --ticket --allow-schema-delete`. Never invent or self-fill that allow flag.
- Kafka uses Confluent Schema Registry when the Kafka context has `--schema-registry-url`; optional `--schema-registry-username` and `--schema-registry-password` use credstore. HTTPS Schema Registry connections use the same TLS SPKI TOFU pin store as broker TLS. Pulsar uses its built-in admin REST schema endpoints and existing token/TLS settings.
- Kafka supports Confluent SR soft delete and hard delete with `--permanent`. Pulsar has no soft/hard distinction: this backend supports permanent subject delete only and returns `NOT_IMPLEMENTED` for soft delete or version delete.
- RabbitMQ and RocketMQ return `NOT_IMPLEMENTED` for schema verbs. Audit may include subject, version, compatibility, and schema hashes, but never schema text or registry credentials.

Fleet governance:

- Fleet verbs are `fleet status|topics`; both are R0 read-only aggregation across configured contexts selected by `--all` or `--contexts a,b,c`.
- Fleet is only fan-out: one batch-read intent precedes all backend access; each selected context still uses its own stored credentials and the same per-context R0 classify/authorize path as the equivalent single-context read; one aggregate outcome precedes dashboard output. Audit stores only a selection fingerprint and bounded counts. Partial backend failures are reported per context as `denied`, `unreachable`, or `error`; an audit persistence failure returns `LOCAL_IO_ERROR` and suppresses the dashboard.
- Never use fleet for cross-broker copy, mirror, migration, or any write path.

DLQ governance:

- DLQ verbs are `dlq list|peek|redrive|purge`. `list` and `peek` are R0 and fingerprint-only; dry-run redrive/purge are R0 previews.
- Real `dlq redrive` is R3 and requires `--allow-internal-produce`; real `dlq purge` is R3 and requires `--allow-topic-purge`.
- RabbitMQ uses DLX-fed queues and supports list/redrive/purge; peek is `NOT_IMPLEMENTED` because consume/requeue cannot prove a non-consuming read. Redrive confirms the publish before acknowledging the source message. Kafka explicit DLQ topics support peek/purge but redrive is `NOT_IMPLEMENTED` because exact bounded copy-and-remove cannot be atomic. Pulsar supports list/peek for `{topic}-{subscription}-DLQ` but refuses redrive/purge because Reader/cursor operations do not provide those deletion semantics. RocketMQ only supports listing `%DLQ%{consumerGroup}` topics.
- Pulsar offset reset supports only `--to latest`, using live per-partition subscription backlog as the affected count. Earliest/datetime/offset/shift resets return `NOT_IMPLEMENTED` before mutation intent because their impact cannot be measured reliably.

Audit:

```bash
mqgov audit query --since 24h --limit 100 -o json
mqgov audit verify --strict -o json
mqgov audit prune --older-than 30 --dry-run -o json
mqgov audit prune --older-than 30 --confirm --yes --ticket <ticket> --allow-audit-prune -o json
```

Confirmed audit pruning is fixed R3 and uses the persisted current-context
policy. It requires `--confirm`, `--yes`, a non-empty ticket, and the exact
allow flag. Never synthesize those authorization inputs. Authenticated v2
history is validated and its checkpoint is advanced by opskit-core/v2 before
the selected oldest rotation prefix is deleted. The prune intent/outcome is
written to the sibling `.<audit-base>-control` log so its rotations never enter
the target evidence namespace.

Every mutation writes an intent before touching the target and an outcome
afterwards. `AUDIT_INCOMPLETE` means outcome durability is incomplete; only an
outcome known not to be committed enters the owner-only
`<audit.log>.outcome-spool`. Do not blindly retry: repair audit storage and
inspect `audit query` / `audit verify` before any replay or manual recovery.
Replay is at-least-once, so a crash after append and before spool deletion can
produce a duplicate outcome with the same `mutationId`. opskit-core/v2 reports
whether an append is committed, not committed, or indeterminate. mqgov only
queues records known not to be committed; committed and indeterminate errors
fail closed without blind replay. An indeterminate replay is renamed with an
`.indeterminate` suffix, blocks later automatic replay, and must be reconciled
by `mutationId`.

All broker reads and mutation preflights write `ReadAuditRecord` intent/outcome
pairs through the same serialized append queue and durable replay spool. Intent
is followed by local R0/context-role authorization before client construction.
Intent/authorization failure prevents backend access; outcome failure suppresses
result output and mutation; audit failure surfaces as `LOCAL_IO_ERROR`,
preserving any backend error in the cause chain.
Only request fingerprints/byte counts and bounded operation/result counts enter
these records. Mutation dry-run/plan preview calls and mutation metadata reads
are included in this mandatory preflight lifecycle.

`message tail` is fingerprint-only and non-destructive. It is supported by Kafka and Pulsar. RabbitMQ and RocketMQ return `NOT_IMPLEMENTED` for tail; both also return `NOT_IMPLEMENTED` for peek.
