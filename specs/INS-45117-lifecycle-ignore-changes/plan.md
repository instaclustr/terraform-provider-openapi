# Implementation Plan: Respect lifecycle ignore_changes on Update

**Branch**: `feature/instaclustr-speckit-releases` | **Date**: 2026-11-25 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/INS-45117-lifecycle-ignore-changes/spec.md`

## Summary

Apply builds its PUT body from the raw HCL config, so attributes listed in `ignore_changes` are sent with the drifted config value even though the plan hid them. The fix detects ignored attributes (config differs from a known plan value), then overlays the value the API currently holds, read by the same `GET` the immutable-field check already makes, onto those attributes in the PUT body. This keeps console changes even with `-refresh=false` (FR-010). See [research.md](research.md).

## Technical Context

**Language/Version**: Go 1.17 (go.mod)

**Primary Dependencies**: terraform-plugin-sdk/v2 v2.16.0, go-cty, testify

**Storage**: N/A (Terraform state only)

**Testing**: `go test` unit tests in `openapi/`, gray-box e2e in `tests/e2e`, integration in `tests/integration`

**Target Platform**: Terraform provider binary (darwin/linux/windows)

**Project Type**: single Go project (Terraform provider generated from OpenAPI)

**Performance Goals**: N/A, one extra structural comparison per update

**Constraints**: No behaviour change when `ignore_changes` is unused (FR-007); do not regress nested computed handling from commit 7471de73; no extra API call (reuse the existing pre-PUT `GET`)

**Scale/Scope**: payload creation path for all generated resources

## Constitution Check

Evaluated against `.specify/memory/constitution.md` v1.0.0.

| Principle | Gate | Pre-research | Post-design |
|---|---|---|---|
| I. Customer-facing behavior | Plan must equal apply; no break for existing configs | PASS (this is the fix) | PASS: contract invariant in `contracts/payload-selection.md`; unused `ignore_changes` leaves payload identical |
| II. Spec is source of truth | Generic, not Kafka-specific | PASS | PASS: logic operates on schema properties, no resource names |
| III. Test-first (non-negotiable) | Failing test before fix; unit + gray-box e2e; `make test` green | PASS | PASS: tasks must order tests (red) before implementation; e2e in `tests/e2e`, including a `-refresh=false` case |
| IV. Minimal, compatible, fork-local | Smallest change, no new deps, gofmt/gosec clean | PASS | PASS: two helpers in `openapi/`, existing `checkImmutableFields` signature kept, no new dependencies or API calls |
| V. Observable | Debug logs without secrets; API errors unchanged | PASS | PASS: helper logs only property names substituted, never values; PUT error path untouched (FR-009) |
| Fork & licensing | Apache 2.0 headers kept; docs for customer-visible change in `docs/` | PASS | PASS: add a note on `ignore_changes` support to `docs/how_to.md` |

No violations; Complexity Tracking stays empty.

## Project Structure

### Documentation (this feature)

```text
specs/INS-45117-lifecycle-ignore-changes/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/payload-selection.md
├── checklists/requirements.md
└── tasks.md             # created by /speckit-tasks
```

### Source Code (repository root)

```text
openapi/
├── resource_factory.go        # update(): read remote once, detect ignored paths, overlay remote values; immutable check takes the remote object
├── common.go                  # new helpers: findIgnoredAttributePaths(config, plan cty.Value, schema), overlayRemoteValues(payload, remote, schema, paths) (log property names only)
├── resource_factory_test.go   # unit tests for payload selection
└── common_test.go             # unit tests for merge helper
tests/e2e/
└── gray_box_ignore_changes_test.go   # new: asserts PUT body with ignored attributes
docs/
└── how_to.md                  # short note: lifecycle ignore_changes is honored on update
```

**Structure Decision**: Keep the change inside `openapi/`. Detection works on `cty.Value`s (keeps null and unknown distinguishable). The payload is built from config exactly as today, then ignored paths are overlaid with values from the remote object that `update()` now reads once and shares with the immutable-field check. `checkImmutableFields(data, client, parentIDs...)` keeps its signature as a wrapper.

## Complexity Tracking

No violations.
