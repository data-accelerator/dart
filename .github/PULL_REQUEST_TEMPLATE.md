<!--
Body must START with "Fixes #N" (or "Refs #N" for a partial step) so the
sidebar links the issue and merge auto-closes it.
-->

Fixes #

## Checklist

- [ ] `go vet ./...` / `go test ./... -race -count=1` / `go test ./... -cover -count=1` all pass (AGENTS.md)
- [ ] `docs/<pkg>.md` updated for any behavior/contract change; `docs/README.md` index row added for a new package
- [ ] `bash scripts/check-docs.sh` passes if `docs/`, `AGENTS.md`, or `CONTRIBUTING.md` changed
- [ ] ADR: ____ (number) / none — reason: ____ (see docs/adr/README.md for the T1–T5 triggers)
