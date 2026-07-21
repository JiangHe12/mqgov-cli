# Contributing

Thank you for contributing. `mqgov-cli` is security-sensitive infrastructure
software, so changes should stay focused, tested, and straightforward to
review.

## Development

Run every gate before submitting changes:

```bash
go build ./...
go test -count=1 ./...
gofmt -l main.go cmd internal   # must print nothing
golangci-lint run --timeout=5m
go vet -tags=integration ./...
CGO_ENABLED=0 go build ./...
go mod tidy -diff
npm pack --dry-run
```

Do not commit credentials, context files, audit logs, broker exports, TLS pins,
or downloaded release binaries.

## Pull Requests

- Keep one behavioral topic per pull request.
- Add adversarial tests for authorization, target resolution, TLS validation,
  compensation, message lifecycle, and backend capability boundaries.
- Update both READMEs, the embedded Skill, and the relevant integration guide
  when user-facing behavior changes.
- Never weaken governance or production authorization to make a test pass.

## Releases

Maintainers release from `main` with `v*` tags. Do not create tags or publish
packages unless explicitly authorized.
