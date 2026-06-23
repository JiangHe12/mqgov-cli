package main

import "embed"

// skillEmbedFS holds the bundled AI skill for `mqgov install <agent> --skills`.
//
//go:embed skills/mqgov-cli
var skillEmbedFS embed.FS
