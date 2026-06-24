---
name: mqgov-cli
description: Governed message middleware operations across Kafka, RabbitMQ, Pulsar, and RocketMQ with R0-R3 authorization, context/credstore support, and fingerprint-only audit.
allowed-tools: Bash(mqgov:*), Bash(mqgov-cli:*)
---

# mqgov-cli

Use `mqgov` for governed message middleware operations. Prefer `-o json` for machine parsing.

Hard rules:

- Never invent or self-fill `--ticket`, `--yes`, or `--allow-*`; these are human authorization inputs.
- Message body, key, and headers must not be copied into audit summaries or tickets. Use fingerprints and counts.
- Check `mqgov capabilities -o json` before assuming a backend supports offsets, partitions, ACL, DLQ verbs, peek, or tail.
- Unsupported backend operations fail closed with `NOT_IMPLEMENTED`.

Common setup:

```bash
mqgov ctx set dev --backend kafka --brokers localhost:9092 --cluster dev
mqgov ctx set dev-rabbit --backend rabbitmq --amqp-url amqp://guest:guest@localhost:5672/ --management-url http://localhost:15672
mqgov ctx set dev-pulsar --backend pulsar --service-url pulsar://localhost:6650 --admin-url http://localhost:8080 --tenant public --pulsar-namespace default
mqgov ctx set dev-rocket --backend rocketmq --nameservers localhost:9876 --broker-addr localhost:10911
mqgov ctx use dev
mqgov ctx test -o json
```

Reads:

```bash
mqgov topic list -o json
mqgov topic describe <topic> -o json
mqgov group list -o json
mqgov group lag <group> <topic> -o json
mqgov message peek <topic> --partition 0 --offset 0 --count 1 -o json
mqgov message tail <topic> --from earliest --max-messages 10 --timeout 30s -o json
mqgov dlq list --topic <topic> --group <group-or-sub> -o json
mqgov dlq peek <dlq> --count 1 -o json
mqgov acl list --principal User:svc -o json
```

Writes require human authorization according to risk:

```bash
mqgov topic create <topic> --partitions 3 --yes -o json
mqgov message produce <topic> --body <text> --yes -o json
mqgov group reset-offset <group> <topic> --to earliest --dry-run -o json
mqgov group reset-offset <group> <topic> --to latest --yes --ticket <ticket> --allow-offset-reset -o json
mqgov topic purge <topic> --dry-run -o json
mqgov topic purge <topic> --yes --ticket <ticket> --allow-topic-purge -o json
mqgov topic delete <topic> --yes --ticket <ticket> --allow-topic-delete -o json
mqgov dlq redrive <dlq> --target <live-topic> --count 100 --dry-run -o json
mqgov dlq redrive <dlq> --target <live-topic> --count 100 --yes --ticket <ticket> --allow-internal-produce -o json
mqgov dlq purge <dlq> --dry-run -o json
mqgov dlq purge <dlq> --yes --ticket <ticket> --allow-topic-purge -o json
mqgov acl grant --principal User:svc --resource-type topic --resource-name <topic> --pattern literal --operation read --permission allow --yes --ticket <ticket> -o json
mqgov acl revoke --principal User:svc --resource-type topic --resource-name <topic> --pattern literal --operation read --permission allow --yes --ticket <ticket> --allow-destructive-acl -o json
mqgov acl grant --principal svc --vhost / --resource-type vhost --resource-name '^orders$' --pattern regex --operation read --permission allow --yes --ticket <ticket> -o json
mqgov acl revoke --principal svc --vhost / --resource-type vhost --resource-name '^orders$' --pattern regex --operation read --permission allow --yes --ticket <ticket> --allow-destructive-acl -o json
mqgov acl grant --principal app-role --resource-type namespace --resource-name public/default --pattern literal --operation produce --permission allow --yes --ticket <ticket> -o json
mqgov acl revoke --principal app-role --resource-type topic --resource-name <topic> --pattern literal --operation consume --permission allow --yes --ticket <ticket> --allow-destructive-acl -o json
```

ACL governance:

- Kafka, RabbitMQ, and Pulsar support `acl list|grant|revoke`; RocketMQ fails closed with `NOT_IMPLEMENTED`.
- Kafka uses broker ACLs with `literal`/`prefixed` patterns. RabbitMQ uses native user-vhost permission regexes with operations `configure`, `write`, and `read`; RabbitMQ rejects deny and non-regex patterns.
- Pulsar uses native role permissions on namespaces or topics with actions `produce`, `consume`, `functions`, `sources`, `sinks`, and `packages`; Pulsar rejects deny and non-literal patterns.
- `acl list` is R0. Normal `acl grant` is R2. Broad grants, including Kafka prefixed patterns, broad RabbitMQ regexes such as `.*`, `.+`, `.`, or `orders.*`, and Pulsar `functions`/`sources`/`sinks`/`packages`, and every `acl revoke` are R3 and require `--allow-destructive-acl`.

DLQ governance:

- DLQ verbs are `dlq list|peek|redrive|purge`. `list` and `peek` are R0 and fingerprint-only; dry-run redrive/purge are R0 previews.
- Real `dlq redrive` is R3 and requires `--allow-internal-produce`; real `dlq purge` is R3 and requires `--allow-topic-purge`.
- Kafka has no native DLQ and never auto-discovers one; use an explicit DLQ topic for peek/redrive/purge. RabbitMQ uses DLX-fed queues. Pulsar uses `{topic}-{subscription}-DLQ`. RocketMQ only supports listing `%DLQ%{consumerGroup}` topics; unsupported DLQ verbs fail closed with `NOT_IMPLEMENTED`.

Audit:

```bash
mqgov audit query --since 24h --limit 100 -o json
mqgov audit verify --strict -o json
```

`message tail` is fingerprint-only and non-destructive. It is supported by Kafka and Pulsar. RabbitMQ and RocketMQ return `NOT_IMPLEMENTED` for tail; RocketMQ also returns `NOT_IMPLEMENTED` for peek.
