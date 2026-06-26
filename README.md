<div align="center">

# mqgov-cli

**Governed message-broker operations for humans _and_ AI agents.**

One safe command line for **Kafka**, **RabbitMQ**, **Pulsar**, and **RocketMQ** — list, describe, peek, tail, produce, govern DLQs, reset offsets, inspect ACLs and schemas, purge, and delete topics without ever fat-fingering production or silently draining a queue.

[![npm version](https://img.shields.io/npm/v/mqgov-cli.svg)](https://www.npmjs.com/package/mqgov-cli)
[![CI](https://github.com/JiangHe12/mqgov-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/JiangHe12/mqgov-cli/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/mqgov-cli.svg)](LICENSE)
[![signed](https://img.shields.io/badge/release-cosign%20%2B%20npm%20provenance-blue.svg)](#-trust--verification)

[English](README.md) · [简体中文](README_zh.md)

</div>

---

## 🧭 What is this? (read me first)

Message brokers — **Kafka**, **RabbitMQ**, **Pulsar**, **RocketMQ** — are the backbone of event-driven systems. The operations against them are deceptively dangerous: **resetting a consumer group's offset** can trigger a reprocessing storm or silently skip unprocessed messages; **purging a topic** or **deleting it** destroys data; **producing to an internal topic** like `__consumer_offsets` can corrupt cluster state. These mistakes are often *silent* — you don't notice until hours later.

**mqgov-cli puts guardrails around every one of those operations.** Think of it as a careful assistant that:

- 🔎 **Shows you the blast radius first** — `--dry-run` / `--plan` print the exact per-partition impact (how many messages an offset reset will replay or skip) before anything happens.
- 🛡️ **Refuses to do something dangerous without explicit sign-off** — risky commands need a confirmation flag, a change ticket, and an explicit `--allow-*` for the operation.
- 👀 **Peeks/tails without consuming** — inspecting or streaming message fingerprints never advances a consumer's position or drains a queue.
- 📜 **Records everything in a tamper-evident audit log** — sha256 fingerprints and counts only, **never your message bodies**.
- 🤖 **Is safe to hand to an AI agent** — the agent can read and preview freely, but **cannot** invent the human approvals required for dangerous actions.

It's built on the shared [`opskit-core`](https://github.com/JiangHe12/opskit-core) governance engine and is part of the **opskit** family of governed CLIs for AI agents.

---

## ✨ Features

| | |
|---|---|
| 📨 **Four brokers** | **Kafka** (franz-go), **RabbitMQ** (AMQP + management API), **Pulsar** (client + admin REST), **RocketMQ** (rocketmq-client-go/v2). One backend-agnostic governance model; pick per context or override per command. |
| 🧱 **topic / group / message / dlq / acl / schema / fleet** | topics: list · describe · create · alter · delete · purge. consumer groups: list · lag · reset-offset. messages: non-destructive peek · tail · bounded mirror · produce. DLQs: list · peek · redrive · purge through native broker models. ACLs: list · grant · revoke where supported. Schemas: list · describe · check · register · delete where native schema registry support exists. Fleet: read-only status and topic inventory across configured contexts. |
| 🔐 **R0–R3 governance** | every operation is risk-classified by the fail-closed `mqclass` engine; protected contexts and internal/system topics escalate one tier; AI callers can never self-authorize. |
| 🎯 **Real blast-radius preview** | `reset-offset --dry-run` and `purge --dry-run` compute the actual per-partition message delta from the live broker — no guessing. The preview is read-only and never mutates. |
| 👀 **Non-destructive peek/tail/mirror source** | inspect, stream, or bounded-copy messages without consuming them or moving any cursor where the broker can guarantee it (Kafka direct reads, Pulsar Reader). Where a broker can't guarantee this, the operation fails closed rather than silently consuming. |
| 🧭 **Honest capabilities** | brokers differ — mqgov reports what each one actually supports (`capabilities -o json`) and **fails closed with `NOT_IMPLEMENTED`** for the rest, never faking it. |
| 📜 **Tamper-evident audit** | hash-chained log of every action (sha256 fingerprints + counts, **no message bodies/keys/headers**); `audit verify` detects tampering. |
| 🔒 **TLS certificate TOFU** | Kafka, RabbitMQ, and Pulsar TLS connections pin the server leaf SPKI-SHA256 on first use in `.mqgov-cli/tls_known_hosts`; a later SPKI mismatch is a hard connection failure. |
| 🩺 **Ops & DX** | backend-bound `ctx` contexts with credstore-backed secrets, `doctor` diagnostics, shell `completion`, OpenTelemetry traces/metrics, JSON output everywhere. |
| 🔏 **Trusted supply chain** | binaries are **cosign-signed**, the npm package ships with **provenance**, and the installer verifies a **SHA-256** checksum. |

### Per-backend capability matrix

| | Kafka | Pulsar | RabbitMQ | RocketMQ |
|---|:---:|:---:|:---:|:---:|
| topic list / describe / create / delete | ✅ | ✅ | ✅ | ✅ |
| produce | ✅ | ✅ | ✅ | ✅ |
| **non-destructive peek** | ✅ | ✅ (Reader) | ✅ (get+requeue) | ❌ `NOT_IMPLEMENTED`¹ |
| **non-destructive tail** | ✅ | ✅ (Reader) | ❌ `NOT_IMPLEMENTED`² | ❌ `NOT_IMPLEMENTED`¹ |
| **offset lag / reset** | ✅ | ✅ (cursor) | ❌ (no offsets) | ❌ |
| alter partitions | ✅ | ✅ | ❌ | ❌ |
| purge | ✅ | ✅ | ✅ | ❌ |
| **DLQ list / peek / redrive / purge** | list ❌; explicit topic peek/redrive/purge ✅ | ✅ `{topic}-{subscription}-DLQ` | ✅ DLX queues | list ✅ `%DLQ%group`; others ❌ |
| **ACL list / grant / revoke** | ✅ | ✅ namespace/topic permissions | ✅ user-vhost permissions | ❌ `NOT_IMPLEMENTED`³ |
| **schema list / describe / check / register / delete** | ✅ Confluent Schema Registry | ✅ built-in admin schema API | ❌ `NOT_IMPLEMENTED` | ❌ `NOT_IMPLEMENTED` |

¹ RocketMQ's Go v2 `PullConsumer` enters the consumer-group lifecycle and commits offsets, so it cannot guarantee non-destructive peek/tail — mqgov fails closed instead of silently advancing offsets. ² RabbitMQ has no forward non-destructive tail because reads are consume/requeue oriented. Unsupported operations always return `NOT_IMPLEMENTED` (exit 12), never a fake success.

³ RocketMQ broker ACLs live in broker-side `plain_acl.yml`, but `rocketmq-client-go/v2` does not expose a public, cgo-free admin API for reading or changing that config. mqgov does not shell out to the Java `mqadmin` tool and does not hand-roll remoting commands; manage RocketMQ ACLs out of band with broker configuration or official mqadmin until the Go client exposes a clean API.

---

## 📦 Install

```bash
npm install -g mqgov-cli
```

This installs a tiny launcher; on first run it downloads the right pre-built binary for your OS/arch from the signed [GitHub Release](https://github.com/JiangHe12/mqgov-cli/releases) and **verifies its SHA-256** before use. Requires Node.js ≥ 14 for the installer (the CLI itself is a self-contained Go binary).

<details>
<summary>Other ways to install</summary>

- **Direct download** — grab the binary for your platform from the [Releases page](https://github.com/JiangHe12/mqgov-cli/releases), verify it against `checksums.txt` (cosign-signed), put it on your `PATH`, and rename it to `mqgov`.
- **From source** — `go install github.com/JiangHe12/mqgov-cli@latest` (Go 1.26+).
- **Mirror / air-gapped** — set `MQGOV_CLI_DOWNLOAD_MIRROR=<base-url>` to fetch the binary from your own mirror.

Verify the install:

```bash
mqgov version
mqgov doctor          # checks context, backend reachability, and audit-log writability
```

</details>

---

## 🚀 Quick start (60 seconds)

```bash
# 1. Point mqgov at your broker (stored as a reusable "context")
mqgov ctx set dev --backend kafka --brokers 127.0.0.1:9092
mqgov ctx use dev
mqgov ctx test                       # ping the broker through the context

# 2. Read something — reads are always free (R0), no flags needed
mqgov topic list -o json
mqgov topic describe orders -o json
mqgov message peek orders --count 5 -o json     # fingerprints only, nothing consumed
mqgov message tail orders --max-messages 10 -o json

# 3. Preview the blast radius of a dangerous op — nothing is changed yet
mqgov group reset-offset billing orders --to latest --dry-run -o json   # shows per-partition delta

# 4. Apply it — an R3 op needs your confirmation, a ticket, AND the allow flag
mqgov group reset-offset billing orders --to latest --yes --ticket OPS-123 --allow-offset-reset

# 5. See what happened
mqgov audit query --since 1h -o json
```

> 💡 **Tip:** mark production contexts with `--protected` when you create them. mqgov then raises the bar for every dangerous operation in that context automatically.

---

## 🔐 The governance model (the important part)

Every command is sorted into one of four **risk tiers** by the fail-closed `mqclass` classifier. The higher the tier, the more explicit human sign-off it needs:

| Tier | What it covers | What you must provide |
|:---:|---|---|
| **R0** | Reads & previews (`topic list/describe`, `group list/lag`, `message peek`, `message tail`, `dlq list/peek`, `acl list`, `schema list/describe/check`, `fleet status/topics`, `*-dry-run`, `audit query/verify`, `doctor`) | Nothing — but it's still audited |
| **R1** | Ordinary writes (`message produce`, target side of `message mirror`, `topic create`, `schema register` for a new subject) | `--yes` (or an interactive confirmation) |
| **R2** | Elevated mutations (`topic alter`, `group create/delete`, `acl grant`, `schema register` for an existing subject, produce/mirror to a **protected** topic) | `--yes` **and** a non-empty `--ticket` |
| **R3** | Destructive / irreversible (`group reset-offset`, `topic purge`, `topic delete`, `schema delete`, `dlq redrive`, `dlq purge`, broad `acl grant`, `acl revoke`, produce/mirror to an **internal/system** topic) | The above **plus** the exact `--allow-*` flag |

The R3 allow flags: `--allow-offset-reset`, `--allow-topic-purge`, `--allow-topic-delete`, `--allow-destructive-acl`, `--allow-internal-produce`, `--allow-schema-delete`.

**Protected contexts, protected topics, and internal/system topics raise the tier by one.** For example, producing to `__consumer_offsets` is treated as a destructive R3 operation and needs `--allow-internal-produce`.

Three rules keep this safe — especially for automation:

1. **Blast radius comes from the tool, not a guess.** Use `--dry-run` / `--plan` to see the exact per-partition impact. Never estimate it by reasoning.
2. **`mqclass` is fail-closed and structure-aware.** All offset changes, purge, topic delete, ACL revoke, and broad ACL grants are pinned R3; wildcard/glob targets escalate; an unknown operation fails closed to the highest tier — it never falls to R0.
3. **🤖 AI agents must never invent `--ticket`, `--allow-*`, or a high-risk `--yes`.** Those are *human* authorization inputs. An agent should surface "this needs approval X" to its operator and stop.

---

## 📚 Command reference

`mqgov <noun> <verb> [flags]`. Add `-o json` for machine-readable output, `--help` on any command for its full flag set, and `mqgov capabilities -o json` to ask the bound backend what it actually supports.

<details open>
<summary><b>topic</b> — topics / queues</summary>

```bash
# Read (R0)
mqgov topic list     [--pattern <name|glob>] -o json
mqgov topic describe <topic> -o json

# Write
mqgov topic create <topic> [--partitions N] --yes                                  # R1 (R2 if protected)
mqgov topic alter  <topic> --partitions N --yes --ticket <t>                       # R2 (Kafka/Pulsar)
mqgov topic purge  <topic> [--dlq] --dry-run                                        # R0 preview
mqgov topic purge  <topic> [--dlq] --yes --ticket <t> --allow-topic-purge          # R3
mqgov topic delete <topic> --yes --ticket <t> --allow-topic-delete                 # R3
```
</details>

<details>
<summary><b>group</b> — consumer groups / subscriptions</summary>

```bash
# Read (R0)
mqgov group list [--pattern <name>] -o json
mqgov group lag  <group> <topic> -o json

# Reset a consumer group's position
mqgov group reset-offset <group> <topic> --to <target> --dry-run -o json           # R0 preview (real per-partition delta)
mqgov group reset-offset <group> <topic> --to <target> --yes --ticket <t> --allow-offset-reset   # R3

#   --to: earliest | latest | offset:N | datetime:<RFC3339> | shift:±N
#   (offset:N / shift:N are Kafka-only; unsupported targets/backends return a clear error)
```

Offsets are a Kafka and Pulsar concept. On RabbitMQ and RocketMQ, `group lag` / `reset-offset` fail closed with `NOT_IMPLEMENTED`.
</details>

<details>
<summary><b>message</b> — peek, tail, mirror & produce</summary>

```bash
mqgov message peek    <topic> [--partition N] [--offset N] [--count N] -o json     # R0, non-destructive, fingerprints only
mqgov message tail    <topic> [--partition N] [--from earliest|latest|offset:N] [--follow] [--max-messages N] [--timeout 30s] -o json
mqgov message mirror  <source-topic> --to-context <ctx> --to-topic <topic> --limit 100 --dry-run -o json
mqgov message mirror  <source-topic> --to-context <ctx> --to-topic <topic> --limit 100 --yes -o json
mqgov message produce <topic> [--key <k>] [--body <text>] --yes                    # R1 (R3 + --allow-internal-produce for internal topics)
```

`peek` and `tail` never consume a message or move a cursor, and return only sha256 fingerprints (`keySha256`, `bodySha256`, size, optional timestamp) — never the body. `tail` is bounded by `--max-messages` and `--timeout`; `--follow` streams new messages only until those bounds or cancellation.

`message mirror` is a bounded one-shot copy, never a daemon. It performs two independent authorizations: a source-side non-destructive read against the source context, then a target-side produce against `--to-context`. `--dry-run` / `--plan` is an R0 preview that reads/counts but does not produce. Kafka and Pulsar can be mirror sources; RabbitMQ and RocketMQ source mirroring fail closed with `NOT_IMPLEMENTED` because their available read APIs cannot guarantee non-destructive full-message reads. Keys, bodies, and headers flow only in process memory; audit records source/target/count and body sha256 aggregation only. Kafka supports `--from earliest|latest|offset:N|timestamp:<RFC3339>` and `--partition`; Pulsar supports `earliest|latest|timestamp:<RFC3339>` and all-partition reads. Headers are copied where both backends can express string/byte headers; unsupported source concepts are not fabricated.
</details>

<details>
<summary><b>dlq</b> — dead-letter queue governance</summary>

```bash
mqgov dlq list [--topic <source-or-dlq>] [--group <group-or-sub>] [--pattern <name|glob>] -o json     # R0
mqgov dlq peek <dlq> [--topic <source>] [--group <group-or-sub>] [--count N] -o json                   # R0, fingerprints only
mqgov dlq redrive <dlq> --target <live-topic> [--count N] --dry-run -o json                            # R0 preview
mqgov dlq redrive <dlq> --target <live-topic> [--count N] --yes --ticket <t> --allow-internal-produce  # R3
mqgov dlq purge <dlq> --dry-run -o json                                                                 # R0 preview
mqgov dlq purge <dlq> --yes --ticket <t> --allow-topic-purge                                           # R3
```

DLQ mapping is backend-native and honest: RocketMQ lists `%DLQ%{consumerGroup}` topics only; RabbitMQ treats DLQs as ordinary queues fed by a dead-letter exchange; Kafka has no native DLQ and never auto-discovers one, so use an explicit DLQ topic for peek/redrive/purge; Pulsar uses `{topic}-{subscription}-DLQ`. Unsupported verbs return `NOT_IMPLEMENTED`.

Redrive is governed as internal produce: dry-run is a read-only preview and real execution requires `--allow-internal-produce`. Audit remains fingerprint/count-only and never stores message body, key, or headers.
</details>

<details>
<summary><b>schema</b> — schema registry</summary>

```bash
mqgov schema list [--pattern <subject>] -o json
mqgov schema describe <subject-or-topic> [--version latest|N] -o json
mqgov schema check <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO [--version latest] -o json
mqgov schema register <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO --yes -o json
mqgov schema register <subject-or-topic> --schema-file ./next.avsc --schema-type AVRO --yes --ticket <t> -o json
mqgov schema delete <subject-or-topic> [--version N] [--permanent] --yes --ticket <t> --allow-schema-delete -o json
```

`schema list`, `schema describe`, and `schema check` are R0 and audited. `schema register` is R1 for a new subject and R2 when the subject already exists; registering a new version is the evolution path. Existing subjects first run the backend compatibility check and incompatible schemas are rejected before registration. `schema delete` is R3 and requires `--allow-schema-delete`. Kafka maps to Confluent Schema Registry, including soft delete and hard delete with `--permanent`. Pulsar maps to its built-in admin schema endpoints; because Pulsar has no soft/hard split, this backend only accepts permanent subject deletion and returns `NOT_IMPLEMENTED` for soft or version delete. RabbitMQ and RocketMQ fail closed with `NOT_IMPLEMENTED`. Audit stores only subject/version metadata and schema hashes, never schema text or registry credentials.
</details>

<details>
<summary><b>fleet</b> — cross-context read-only views</summary>

```bash
mqgov fleet status --all -o json
mqgov fleet topics --contexts dev,staging --pattern orders -o json
```

`fleet status` fans out `Ping`, `Describe`, and `Capabilities` across selected contexts. `fleet topics` fans out topic listing and tags every row with its source context. Select contexts with exactly one of `--all` or `--contexts a,b,c`. Fleet is R0 only: each per-context read still runs through the same R0 classification and authorization path as a single-context command, using that context's own stored credentials. Partial failures are reported per context as `denied`, `unreachable`, or `error` data and the command still exits 0.
</details>

<details>
<summary><b>acl</b> — broker access control</summary>

```bash
mqgov acl list [--principal <P>] [--resource-type <T>] [--resource-name <N>] -o json

# Kafka broker ACLs
mqgov acl grant --principal User:svc --resource-type topic --resource-name orders \
  --pattern literal --operation read --permission allow --yes --ticket <t>

mqgov acl revoke --principal User:svc --resource-type topic --resource-name orders \
  --pattern literal --operation read --permission allow \
  --yes --ticket <t> --allow-destructive-acl

# RabbitMQ native user-vhost permissions
mqgov acl grant --principal svc --vhost / --resource-type vhost --resource-name '^orders$' \
  --pattern regex --operation read --permission allow --yes --ticket <t>

mqgov acl revoke --principal svc --vhost / --resource-type vhost --resource-name '^orders$' \
  --pattern regex --operation read --permission allow \
  --yes --ticket <t> --allow-destructive-acl

# Pulsar native namespace/topic permissions
mqgov acl grant --principal app-role --resource-type namespace --resource-name public/default \
  --pattern literal --operation produce --permission allow --yes --ticket <t>

mqgov acl revoke --principal app-role --resource-type topic --resource-name orders \
  --pattern literal --operation consume --permission allow \
  --yes --ticket <t> --allow-destructive-acl
```

`acl list` is R0 and audited. Normal `acl grant` is R2. Broad grants (Kafka prefixed pattern, wildcard principal, wildcard resource, cluster resource, `all`, `alter`, cluster-action style operations, broad RabbitMQ regexes such as `.*`, `.+`, `.`, and `orders.*`, or Pulsar `functions`/`sources`/`sinks`/`packages`) and every `acl revoke` are R3 and require `--allow-destructive-acl`. Kafka implements broker ACLs with `literal`/`prefixed` patterns. RabbitMQ maps ACLs to native per-user, per-vhost permission regexes (`configure`, `write`, `read`) and only supports `--permission allow` with `--pattern regex`. Pulsar maps ACLs to native role permissions on namespaces or topics with actions `produce`, `consume`, `functions`, `sources`, `sinks`, and `packages`; it is allow-only and uses `--pattern literal`. RocketMQ fails closed with `NOT_IMPLEMENTED`: broker ACLs are managed through broker-side `plain_acl.yml` or the official Java `mqadmin`, because `rocketmq-client-go/v2` exposes no clean public ACL admin API.
</details>

<details>
<summary><b>ctx</b>, <b>audit</b>, <b>doctor</b> & friends</summary>

```bash
# Backend-bound contexts (credentials go through credstore, never plaintext)
mqgov ctx set <name> --backend kafka    --brokers <h:p,h:p> [--sasl-mechanism PLAIN] [--tls --ca-cert <f>] [--schema-registry-url <url>] [--schema-registry-username <u>] [--schema-registry-password <p>] [--protected]
mqgov ctx set <name> --backend rabbitmq (--amqp-url <url> | --host <h> --port <p> --vhost </>) --management-url <url> --username <u>
mqgov ctx set <name> --backend pulsar   --service-url pulsar://<h:p> --admin-url http://<h:p> [--tenant public] [--pulsar-namespace default]
mqgov ctx set <name> --backend rocketmq --nameservers <h:p,h:p> [--broker-addr <h:p>]
mqgov ctx use|list|current|delete|test
#   secrets: --password <pw|token|secretKey> and --schema-registry-password <pw> go through --credential-backend <encrypted-file|keychain|...>  (a non-plain backend is required)
#   RabbitMQ: prefer --username plus MQGOV_PASSWORD for non-interactive runs; to persist a password, use --password with keychain or encrypted-file.
#   If --amqp-url contains userinfo, explicit --username and password sources override it for both AMQP and management API auth.

# Audit (tamper-evident, fingerprint-only)
mqgov audit query  [--since 24h] [--type <t>] [--operator <o>] [--status <s>] [--limit 100] -o json
mqgov audit verify [--strict] -o json

# Diagnostics & ecosystem
mqgov doctor -o json            # read-only health check (redacted output)
mqgov capabilities -o json      # what the bound backend supports
mqgov completion bash|zsh|fish|powershell
mqgov install <agent> --skills  # install the mqgov AI skill into an agent (claude, codex, …) or a custom path
mqgov version
```
</details>

---

## 🤖 For AI agents

mqgov-cli is designed to be driven by autonomous agents safely:

- Run `mqgov capabilities -o json` first to discover what the bound backend supports — brokers differ, don't assume (e.g. Kafka, RabbitMQ, and Pulsar support `acl` with different native models; Kafka and Pulsar support `schema`; RabbitMQ/RocketMQ have no offsets, schema registry, or tail; RocketMQ has no peek). Use `fleet status --all -o json` for a read-only cross-context dashboard.
- Use `-o json` everywhere; every command returns a stable, versioned envelope.
- Get blast radius from `--dry-run` / `--plan`, never from your own reasoning.
- **Never self-fill `--ticket`, `--allow-*`, or a high-risk `--yes`.** Surface the required human approval and stop.

Install the bundled skill into your agent so it learns these rules automatically:

```bash
mqgov install claude --skills     # also: codex, opencode, copilot, cursor, windsurf, aider, cc-switch
```

---

## 🔏 Trust & verification

- **Signed binaries** — every release artifact is signed with [cosign](https://github.com/sigstore/cosign) (keyless / OIDC). A `checksums.txt` (also signed) covers all platforms.
- **npm provenance** — the npm package is published from CI via OpenID Connect with [provenance attestations](https://docs.npmjs.com/generating-provenance-statements) linking it to this exact repo and workflow.
- **Verified installs** — the npm postinstall downloads the binary over an allow-listed host and checks its SHA-256 against the signed `checksums.txt` before installing.
- **Tamper-evident audit** — `mqgov audit verify --strict` re-walks the hash chain and reports any gap or modification.
- **No insecure transport** — SASL/TLS and mTLS only; mqgov never offers an insecure-skip-verify escape hatch. TLS broker/admin/Schema Registry connections for Kafka, RabbitMQ, and Pulsar add TOFU SPKI-SHA256 pinning on top of normal CA validation; pins live in `.mqgov-cli/tls_known_hosts`.

---

## 🏗️ Build from source & contribute

```bash
git clone https://github.com/JiangHe12/mqgov-cli && cd mqgov-cli
go build ./...
go test -count=1 ./...
gofmt -l main.go cmd internal      # must print nothing
golangci-lint run --timeout=5m
go vet -tags=integration ./...
```

Real-backend integration tests (`//go:build integration`, env-gated, skipped by default) run against live Kafka/RabbitMQ/Pulsar/RocketMQ containers in the nightly `integration.yml` workflow; see [`docs/`](docs/) for how to run them locally with the bundled `docker-compose.*.yml` files.

mqgov-cli is built on the shared [`opskit-core`](https://github.com/JiangHe12/opskit-core) governance engine and is part of the **opskit** family of governed CLIs for AI agents — alongside [`dbgov-cli`](https://www.npmjs.com/package/dbgov-cli) (databases), [`srvgov-cli`](https://www.npmjs.com/package/srvgov-cli) (remote servers), and [`cfgov-cli`](https://www.npmjs.com/package/cfgov-cli) (config centers).

---

## 📄 License

[MIT](LICENSE) © JiangHe12
