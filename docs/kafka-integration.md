# Kafka integration test

Start a local single-node Redpanda broker:

```sh
docker compose -f docker-compose.kafka.yml up -d
```

The same container also exposes Redpanda's built-in, Confluent-compatible Schema
Registry on `:8081`, which backs the `schema` verb tests.

Run the env-gated integration test:

```sh
KAFKA_BROKERS=127.0.0.1:9092 \
KAFKA_SCHEMA_REGISTRY_URL=http://127.0.0.1:8081 \
go test -tags=integration -count=1 ./internal/backend/kafka
```

`KAFKA_SCHEMA_REGISTRY_URL` is optional; omit it to skip only the schema-registry
checks. The default `go test ./...` path does not require Kafka and skips this
file because it is build-tagged.

> The Kafka ACL round-trip runs in CI against a TLS+SASL+authorizer broker
> (`docker-compose.kafka-acl.yml`, gated on `KAFKA_ACL_*`); it is not part of this
> plain-broker setup.
