# mqgov-cli Agent Guide

`mqgov-cli` is the governed message middleware CLI in the opskit family.

## Scope

- P0 contains only the broker-agnostic governance spine and an in-memory fake broker.
- Do not add real broker clients in P0.
- Reuse `github.com/JiangHe12/opskit-core` for errors, context, credential storage, safety authorization, audit, redaction, printing, telemetry, and lockfile behavior.
- Keep `AGENTS.md` and `CLAUDE.md` identical.

## Governance boundaries

- R0 reads are unauthenticated but audited.
- R1 requires `--yes`.
- R2 requires `--yes` and a non-empty human-supplied `--ticket`.
- R3 requires `--yes`, `--ticket`, and the precise `--allow-*` flag.
- Never weaken `mqclass` to make tests pass. Unknown or ambiguous operations fail closed to the highest safe risk.
- Offset changes, purge, delete, wildcard/glob targets, protected topics, internal/system topics, and protected contexts must preserve the risk escalation rules.
- Message body, key, and headers must never be written to audit. Use fingerprints and counts/sizes only.

## Build & Verify

Run all gates before reporting done:

```sh
go build ./...
go test -count=1 ./...
gofmt -l main.go cmd internal
golangci-lint run --timeout=5m
go vet -tags=integration ./...
```

`gofmt -l main.go cmd internal` must print nothing.
