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

RocketMQ's public create API is an upsert, so `topic create` is conservatively classified R2 instead of the R1 used by create-only backends. Protected targets/contexts escalate it to R3 and require the exact `--allow-topic-upsert` flag. Before dispatch, the backend queries every configured name server separately and fails closed unless each reports the topic absent. After the create RPC returns without a client-reported error, it queries each name server separately between bounded retry intervals and succeeds only when every route reports the requested queue count. It never fabricates the returned count from the request. The upstream v2.1.2 client ignores the route-call context and applies fixed internal route/write timeouts, so an in-flight admin call can outlive the configured command timeout; cancellation is guaranteed before and between attempts, not as a hard interrupt of that upstream call. Any create RPC error, queue-count conflict, or later confirmation failure is reported as `PARTIAL_FAILURE` and audits one uncertain mutation because the request may already have taken effect. The pinned real RocketMQ job is a required release gate and also runs nightly/manually.

This backend intentionally implements only the operations that `github.com/apache/rocketmq-client-go/v2` can report honestly: topic create/list/describe and produce. Topic delete is `NOT_IMPLEMENTED`: the client ignores broker and name-server response codes, and name-server route disappearance alone cannot prove broker-side deletion. Namespace is also rejected because the client applies namespace wrapping inconsistently across route lookup, produce, create, delete, and list. Peek/tail, offset reset, lag, alter, purge, and ACL management are likewise not advertised as supported capabilities and fail closed with `NOT_IMPLEMENTED` through the governance layer.
