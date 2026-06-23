# Pulsar integration test

Start a local standalone Pulsar broker:

```sh
docker compose -f docker-compose.pulsar.yml up -d
```

Run the env-gated integration test:

```sh
PULSAR_SERVICE_URL=pulsar://127.0.0.1:6650 \
PULSAR_ADMIN_URL=http://127.0.0.1:8080 \
go test -tags=integration -count=1 ./internal/backend/pulsar
```

The default `go test ./...` path does not require Pulsar and skips this file because it is build-tagged.

Pulsar peek uses a Reader starting at the earliest message ID. A Reader does not belong to a subscription and does not acknowledge messages or advance subscription cursors; the backend returns only fingerprints.

Known limitation: `reset-offset --dry-run` impact for `earliest` and `datetime:<RFC3339>` is based on the subscription backlog reported by Pulsar stats. Pulsar stats do not expose the subscription cursor's absolute position directly, so previews for rewinding/replay can under-report the number of messages that would become visible again. `reset-offset --to latest` impact is accurate because it clears the current backlog. Real resets remain behind the normal R3 authorization gate and require human review.
