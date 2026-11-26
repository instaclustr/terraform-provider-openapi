---
description: "Task list for INS-45117 respect lifecycle ignore_changes on update (design revision 2, FR-010)"
---

# Tasks: Respect lifecycle ignore_changes on Update

**Input**: Design documents from `specs/INS-45117-lifecycle-ignore-changes/`
**Prerequisites**: plan.md, spec.md (incl. Clarifications), research.md (rev 2), data-model.md, contracts/payload-selection.md
**Tests**: REQUIRED (constitution principle III: failing test first, then fix)
**Tooling note**: do NOT run `make test`/`make fmt` (they reformat unrelated files and `make lint` runs `go get -u`). Use `go vet`, `go test`, and `gofmt -l <touched files>`; lint/gosec from the scratchpad binaries with `GOTOOLCHAIN=go1.26.0`. Code must stay Go 1.17 compatible (`interface{}`, no generics).

## Format: `- [ ] [ID] [P?] [Story] Description with file path`

## Phase 1: Setup

- [X] T001 Create working branch `bugfix/ins-45117-lifecycle-ignore-changes` and record baseline (go vet clean, golint clean, `go test ./openapi/` passes; gosec latest reports pre-existing findings in untouched code)
- [X] T002 [P] Read `openapi/resource_factory.go` (`update`, `checkImmutableFields`, `createPayloadFromTerraformConfig`, `populatePayload`) and `openapi/common.go` to confirm the root cause in research.md

## Phase 2: Foundational (blocks all stories)

Replaces the earlier skeletons (old design) with the revision 2 design.

- [X] T003 [P] In `openapi/common.go` REMOVE the old skeletons `mergePlanIntoConfigValue` and `shouldUsePlanValue`, and ADD: type `ignoredAttribute struct { Path []string; PlanValue interface{} }` (Path = Terraform attribute names, list indexes as decimal strings), skeleton `findIgnoredAttributePaths(config, plan cty.Value, schema *SpecSchemaDefinition) []ignoredAttribute` (returns nil; the schema gives access to each property's computed flag, FR-011), and skeleton `overlayRemoteValues(payload map[string]interface{}, remote map[string]interface{}, schema *SpecSchemaDefinition, ignored []ignoredAttribute)` (no-op). Doc comments state the rule from data-model.md: ignored = "config known and non-null, plan known and non-null, plan != config", plus "config null, plan known and non-null, attribute not computed" (FR-011), compared with `cty.Value` `IsNull`/`IsKnown`/`RawEquals`
- [X] T004 In `openapi/resource_factory.go` (after T003, it uses the `ignoredAttribute` type) refactor with NO behaviour change: add `checkImmutableFieldsWithRemote(data *schema.ResourceData, remoteData map[string]interface{}, ignored []ignoredAttribute) error` (final signature; `ignored` is nil until T013, and attributes in it are skipped by the comparison) holding the current comparison and rollback logic; make `checkImmutableFields(data, client, parentIDs...)` read remote and delegate (signature unchanged, existing test at `openapi/resource_factory_test.go` still compiles); in `update()` call `readRemote` once before building the payload and pass the result to `checkImmutableFieldsWithRemote`. Confirm `go test ./openapi/` still passes

- [X] T004a Add a test helper in `openapi/helperutils_test.go`: `applyUpdateWithRaw(t, resource *schema.Resource, priorState map[string]string, rawConfig, rawPlan cty.Value, client ClientOpenAPI) error` that calls `schema.Resource.Apply` with a `terraform.InstanceDiff{RawConfig, RawPlan, Attributes}` so the resource's `update` runs against a `ResourceData` whose `GetRawConfig()`/`GetRawPlan()` return the given values; plus a stub client (extend the stub in `openapi/openapi_client_stub.go` if needed) whose `Get` returns a configurable remote object, whose `Put` records the request payload, and which counts `Get`/`Put` calls. Add one smoke test proving `GetRawPlan()` differs from `GetRawConfig()` inside `update`. Spike result: `Resource.Apply` was not needed; `res.Data(&terraform.InstanceState{ID, Attributes, RawConfig, RawPlan})` carries the raw values into `GetRawConfig()`/`GetRawPlan()`, and the helper then calls `r.update` directly (helpers `newResourceDataWithRaw`, `applyUpdateWithRaw`, `testCtyValue`; use a read-only identifier property in tests, as `id` is not part of the factory schema). Prerequisite of T005–T008 and T015–T016

**Checkpoint**: compiles, all existing tests pass, no behaviour change, test helper works.

## Phase 3: User Story 1 - Update while ignoring drifted attributes (P1) 🎯 MVP

**Goal**: Apply sends the API's current value for ignored attributes (also with `-refresh=false`) and still applies other changes.
**Independent Test**: config and plan differ on attribute A (ignored), B changes; the PUT body has A's current API value and B's new value.

### Tests first (must FAIL before implementation)

- [X] T005 [P] [US1] Unit tests for `findIgnoredAttributePaths` in `openapi/common_test.go`: top-level primitive ignored; sibling changed (not ignored); null config + non-computed + plan set → ignored (FR-011); null config + computed + plan set → not ignored; unknown plan not ignored; null plan not ignored; int vs float representation; no differences returns empty
- [X] T006 [P] [US1] Unit tests for `overlayRemoteValues` in `openapi/common_test.go`: remote value replaces payload value; Terraform name translated to API property name via `GetTerraformCompliantPropertyName` (e.g. `node_size` vs `nodeSize`); missing remote value falls back to `ignoredAttribute.PlanValue`; empty ignored list leaves payload untouched
- [X] T007 [P] [US1] Unit tests for `update()` in `openapi/resource_factory_test.go` (near `TestUpdate`) with a stub client capturing the PUT body: (a) config ≠ plan on ignored attr, PUT carries remote value and the changed attr carries its new value; (b) ignored attribute that the API requires on every update is still sent, with the remote value (FR-003); (c) stale state case: plan/prior state differ from remote (simulates `-refresh=false`), PUT carries the remote value (FR-010); this is the AUTHORITATIVE test for FR-010 and acceptance scenario 5; (d) the stub records exactly one GET before the PUT; (e) ignored attribute absent from config (FR-011): PUT carries the remote value, not an empty or zero value
- [X] T008 [P] [US1] Gray-box e2e test `TestAcc_IgnoreChanges` in `tests/e2e/gray_box_ignore_changes_test.go` following `tests/e2e/gray_box_plugin_test.go` (stub API data under `tests/e2e/data/`): resource with `lifecycle { ignore_changes = [attr_a] }`; step 2 changes `attr_a` and `attr_b`, then asserts the stub received the remote `attr_a` and new `attr_b`, and that state after apply keeps the remote `attr_a` (no "inconsistent result" error); add a variant where `attr_a` is removed from the HCL while still in `ignore_changes` (FR-011), asserting the PUT carries the remote `attr_a`; step 3 (best effort, acceptance scenario 5) changes the stub's `attr_a` out of band and applies with `-refresh=false` (use `ExpectNonEmptyPlan`/plan options as supported), asserting the PUT carries the stub's current value. NOTE: the SDK `TestStep` has no `-refresh=false` option; if step 3 cannot be expressed (for example by seeding the stub with a value that differs from state before the step), keep steps 1-2 and rely on T007 case (c) as the authoritative proof of FR-010, recording the limitation in a code comment
- [X] T009 [US1] Run T005–T008 and confirm each fails for the expected reason (config value sent)

### Implementation

- [X] T010 [US1] Implement `findIgnoredAttributePaths` for objects and primitives in `openapi/common.go` per the rule in data-model.md (compare `cty.Value`s; recurse into object attributes; leaf converts the plan sub-value for `PlanValue` with `getTerraformConfigObject`)
- [X] T011 [US1] Implement `overlayRemoteValues` for top-level and nested object paths in `openapi/common.go`, writing API property names, falling back to `PlanValue` when the remote has no value at the path
- [X] T012 [US1] Wire it in `update()` in `openapi/resource_factory.go`: after `readRemote`, compute `ignored := findIgnoredAttributePaths(data.GetRawConfig(), data.GetRawPlan())`, build the payload as today, then `overlayRemoteValues(requestPayload, remote, schema, ignored)`; log only attribute names at `[DEBUG]`, never values (constitution V)
- [X] T013 [US1] Make the immutable-field check skip attributes in `ignored` (research.md "Open point"): pass `ignored` to `checkImmutableFieldsWithRemote` (signature already final from T004), keep the wrapper's behaviour for callers without ignored paths
- [X] T014 [US1] Run T005–T008 and `go test ./openapi/...`; all pass

**Checkpoint**: reported Kafka scenario and console-drift scenario fixed for top-level and object attributes.

## Phase 4: User Story 2 - Plan and apply stay consistent, nested and lists (P1)

**Goal**: Ignored attributes inside objects and lists are honored while siblings still change; `ignore_changes = all` works.
**Independent Test**: an ignored nested attribute keeps its API value while a sibling in the same object or list item changes.

### Tests first

- [X] T015 [P] [US2] Unit tests in `openapi/common_test.go` for `findIgnoredAttributePaths` and `overlayRemoteValues`: nested object; list of objects per index with equal lengths; length mismatch replaces whole list with remote list; list property with `x-terraform-ignore-order` replaces whole list; single-item-list objects (legacy block approach, `shouldUseLegacyTerraformSDKBlockApproachForComplexObjects`); remote list shorter than config
- [X] T016 [P] [US2] Gray-box e2e cases in `tests/e2e/gray_box_ignore_changes_test.go`: `ignore_changes = [nested_obj[0].field]` with a sibling change; `ignore_changes = all` (done as `TestAcc_IgnoreChangesAll`: with `all` every change is ignored, so the assertion is that no PUT is sent; a sibling change cannot be applied at the same time); assert PUT body
- [X] T017 [US2] Run T015–T016 and confirm they fail

### Implementation

- [X] T018 [US2] Extend `findIgnoredAttributePaths` and `overlayRemoteValues` in `openapi/common.go` for lists per research.md decision 2 (per index when lengths match and not ignore-order; whole-list replacement otherwise)
- [X] T019 [US2] Add a regression test in `openapi/resource_factory_test.go` for nested computed properties in lists (the case fixed by commit 7471de73) proving they are not marked ignored
- [X] T020 [US2] Run T015–T019 and `go test ./openapi/...`; all pass

## Phase 5: User Story 3 - Unchanged behavior when not used (P2)

**Goal**: No behaviour change without `ignore_changes`; removing an ignore re-manages the attribute; edge cases hold.
**Independent Test**: existing suites pass unchanged; removing the ignore sends the new value.

- [X] T021 [P] [US3] Unit test in `openapi/resource_factory_test.go`: when plan equals config for every property, the PUT payload is byte-identical to the pre-change output (table of existing fixtures) (FR-007)
- [X] T022 [P] [US3] Gray-box e2e in `tests/e2e/gray_box_ignore_changes_test.go`: step removes the attribute from `ignore_changes`, the new value is sent (FR-008)
- [X] T023 [P] [US3] Unit test in `openapi/resource_factory_test.go`: ignored immutable attribute with config ≠ remote (incl. stale-state case) does not abort the update and the PUT carries the remote value; a non-ignored immutable attribute still aborts with the existing error
- [X] T024 [P] [US3] E2E test in `tests/e2e/gray_box_ignore_changes_test.go`: only ignored attributes differ, plan is empty and the stub records no PUT (FR-006)
- [X] T025 [P] [US3] Unit test in `openapi/resource_factory_test.go`: an API error unrelated to ignored attributes is returned unchanged (FR-009)
- [X] T026 [US3] Run the full suite `go test ./...` (excluding integration packages as the Makefile does) and confirm no existing test was modified or broken

## Phase 6: Polish & Cross-Cutting

- [X] T027 [P] Add a short "lifecycle ignore_changes" note to `docs/how_to.md`: ignored attributes keep their current API value, console changes are preserved, `-refresh=false` is supported
- [X] T028 [P] Quality gates on touched files only: `gofmt -l`, `go vet ./...`, golint and gosec with `GOTOOLCHAIN=go1.26.0` from the scratchpad binaries; no new gosec findings versus the baseline table in `research.md` ("Quality-gate baseline")
- [X] T029 Execute the manual validation in `specs/INS-45117-lifecycle-ignore-changes/quickstart.md` against the stub API
- [X] T030 Prepare the PR: description with customer impact, INS-45117 link, test output, note that `make test` was not used and why

## Dependencies & Order

- Phase 1 → Phase 2 (T003, T004, then T004a) → US1 (MVP) → US2 → US3 → Polish.
- T003 then T004 (T004 uses the `ignoredAttribute` type from T003). T004a (test helper) follows T004 and blocks every test task (T005–T008, T015–T016). US2 extends the helpers from US1 (same file) so it follows US1. T021–T025 can be written after Phase 2 but run last.
- Within each story: tests (red) → implement → green.

## Parallel Examples

- US1 tests T005, T006 (same file, separate functions, write together), T007, T008 are different files and parallel.
- US3 tests T021–T025 are parallel.
- Polish T027, T028 are parallel.

## Implementation Strategy

1. MVP = Phases 1–3 (fixes the reported case and the console-drift case with `-refresh=false`; ship-able alone).
2. Add US2 for nested and list coverage, then US3 for regression and edge-case guarantees.
3. Stop at each checkpoint, run `go test ./openapi/...`.
