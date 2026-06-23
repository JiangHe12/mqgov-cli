# Kafka integration test

Start a local single-node Redpanda broker:

```sh
docker compose -f docker-compose.kafka.yml up -d
```

Run the env-gated integration test:

```sh
KAFKA_BROKERS=127.0.0.1:9092 go test -tags=integration -count=1 ./internal/backend/kafka
```

The default `go test ./...` path does not require Kafka and skips this file because it is build-tagged.
