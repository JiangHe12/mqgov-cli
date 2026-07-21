# Security Policy

## Supported Versions

Security fixes target the latest release. Upgrade to the newest version when a
security update is published.

## Reporting A Vulnerability

Report vulnerabilities privately through GitHub Security Advisories:

<https://github.com/JiangHe12/mqgov-cli/security/advisories/new>

Do not publish exploit details before a coordinated fix is available. Include
the affected version, platform, broker, impact, reproduction steps, and a
suggested mitigation when possible.

## Trust Boundary

`mqgov-cli` trusts the current OS user, owner-controlled files under `~/.mqgov-cli`,
explicit credential backends, configured CA roots and host pins, and release
artifacts from the canonical GitHub repository. It does not trust broker or
admin responses, imported files, user-provided URLs, npm mirrors, or
model-generated authorization values.

## Governance And Data Handling

- R0 reads and previews are audited. R1 requires confirmation, R2 adds a human
  ticket, and R3 adds the exact operation-specific `--allow-*` flag.
- Context, role, credential, and audit-evidence controls use fixed R3
  authorization and the pre-change policy.
- Confirmed audit pruning requires `--confirm`, `--yes`, a non-empty ticket,
  and the exact `--allow-audit-prune`; neither confirmation form substitutes
  for the other.
- AI agents must not synthesize tickets, allow flags, or high-risk confirmation.
- Unknown metadata and unsupported broker semantics fail closed; impact comes
  from broker-backed preview data, never from a caller estimate.
- Mutation audit and telemetry store fingerprints and bounded metadata, not raw
  tickets, reasons, message bodies, credentials, target values, or broker error
  text.
- Prefer keychain or encrypted-file storage and protect context, TLS-pin, and
  audit files with owner-only access.

The trusted local identity is the OS username plus hostname. An AI process and
a human sharing that OS account are not separated by the CLI; stronger approval
requires an external verifier or a separately protected operator identity.

## Supply Chain

Release binaries are built by GitHub Actions, signed, and published with
checksums. Use canonical releases and do not disable installer verification in
production automation.
