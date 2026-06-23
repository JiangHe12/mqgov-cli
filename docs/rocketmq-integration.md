# RocketMQ integration test

Start a local RocketMQ name server and broker:

```sh
docker compose -f docker-compose.rocketmq.yml up -d
```

Run the env-gated integration test:

```sh
ROCKETMQ_NAMESRV_ADDR=127.0.0.1:9876 \
ROCKETMQ_BROKER_ADDR=127.0.0.1:10911 \
go test -tags=integration -count=1 ./internal/backend/rocketmq
```

The default `go test ./...` path does not require RocketMQ and skips the integration file because it is build-tagged.

This backend intentionally implements only the operations cleanly supported by `github.com/apache/rocketmq-client-go/v2`: topic create/delete/list/describe, produce, and non-destructive `PullConsumer.PullFrom` peek. Offset reset, lag, alter, purge, and ACL management are not advertised as supported capabilities and fail closed through the existing governance layer.

RocketMQ peek uses `PullFrom` against an explicit queue and offset. The backend does not call `UpdateOffset` or `PersistOffset`, and returns only fingerprints.
