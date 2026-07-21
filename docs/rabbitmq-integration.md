# RabbitMQ integration test

Start a local RabbitMQ broker with the management plugin:

```sh
docker compose -f docker-compose.rabbitmq.yml up -d
```

Run the env-gated integration test:

```sh
RABBITMQ_AMQP_URL=amqp://guest:guest@127.0.0.1:5672/%2F \
RABBITMQ_MANAGEMENT_URL=http://127.0.0.1:15672 \
go test -tags=integration -count=1 ./internal/backend/rabbitmq
```

The default `go test ./...` path does not require RabbitMQ and skips this file because it is build-tagged.

RabbitMQ peek uses `basic.get` with `autoAck=false` and holds each distinct delivery until the requested batch has been fingerprinted. It then requeues the whole batch together. Messages remain in the queue, but RabbitMQ may mark them as redelivered and may change their ordering.
