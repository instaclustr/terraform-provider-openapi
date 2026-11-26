# Research: INS-45117 lifecycle ignore_changes on Update

Revision 2 (2026-11-26): reworked for FR-010 (clarification: use the API's current value for ignored attributes, even with `-refresh=false`).

## Root cause (from code reading)

- `resourceFactory.update` (`openapi/resource_factory.go`) builds the PUT body with `createPayloadFromTerraformConfig`, which reads `data.GetRawConfig()`, i.e. the **user's HCL**, not the plan.
- Terraform core implements `lifecycle.ignore_changes` by substituting the **prior state** value into the **planned** value for ignored attributes. The plan hides the diff, and `data.Get()` / `data.GetRawPlan()` carry the prior value. `GetRawConfig()` is never altered, so it still holds the new HCL value.
- Result: apply sends the config value for ignored attributes, contradicting the plan; CMAPI rejects it, or a change made in the console is silently reverted.
- `checkImmutableFields` already performs a `GET` of the remote object just before the PUT, and compares it with plan/state values. An ignored immutable attribute therefore passes the guard, yet the PUT still carries the config value.
- `updateStateWithPayloadDataAndOptions` (`openapi/common.go`) also reads `GetRawConfig()`, but only as a hint for ordering and conversion; state is overwritten from the remote response. Out of scope unless tests show otherwise.

## Decision 1 (reworked): detect from plan vs config, take the value from the API

- **Detection**: an attribute is *ignored* when plan is known and non-null and plan != config (compared on `cty.Value` with `RawEquals`, before any conversion so null/unknown are not confused with zero values), and either config is known and non-null, or config is null and the attribute is not computed (FR-011; for non-computed attributes null config with a non-null plan can only come from `ignore_changes`). The resource schema is therefore an input to detection. Applied recursively to objects and lists. Output: a list of attribute paths (Terraform names, with list indexes).
- **Value**: for each ignored path, the PUT body carries the value the API currently holds, taken from the remote object read just before the update (FR-003, FR-010). Fallback when the remote object has no value at that path (for example write-only or sensitive properties the API never returns): use the plan value (prior state).
- **Rationale**: FR-010 says the sent value must be correct even when state is stale (`-refresh=false`). The plan value is the stale prior state in that case; only the live `GET` is current. Detection still works with stale state because plan (stale state) != config.
- **Alternatives considered**:
  - Use the plan value for ignored attributes (previous design): fails FR-010; with `-refresh=false` it could revert a console change.
  - Force a second `GET` just for this: unnecessary, `checkImmutableFields` already does one.
  - Switch the whole payload to remote or plan data: breaks FR-007 and the nested-computed behaviour fixed by commit 7471de73.

## Decision 1b: overlay on the payload, in API shape

- **Decision**: Build the payload exactly as today (config → `populatePayload`), then **overlay** remote values at the ignored paths. Paths are translated from Terraform names to API property names through the resource schema (`GetTerraformCompliantPropertyName`). The remote object is already in API shape (JSON-decoded), so no cty conversion of substituted values is needed.
- **Rationale**: Smallest change to existing code, zero effect when there are no ignored paths (FR-007), and avoids the null/unknown loss in `getTerraformConfigObject` because detection is done on cty values.
- **Alternatives considered**: Merge on cty then convert (earlier design, analysis U1): still right for detection, but cannot carry remote values since remote is not a cty value.

## Decision 2: lists and nested objects

- **Decision**: Objects are compared per key. Lists of objects are compared per index when plan and config lengths match; the overlay then takes the remote element at the same index. If lengths differ, or the list property sets `x-terraform-ignore-order`, the **whole list** is treated as ignored and replaced by the remote list (order of the API's own list is authoritative).
- **Rationale**: Terraform allows ignoring a whole attribute or an indexed element. Remote lists may be ordered differently from config, so per-index overlay is only safe when sizes match and order is not ignorable.
- **Alternatives considered**: Whole-top-level-attribute substitution only. Rejected: it would drop legitimate sibling changes in the same object (FR-004/FR-005).

## Decision 3: restructure `update()` to read the remote object once

- **Decision**: In `update()`, call `readRemote` once, before building the payload. Pass the remote object to both the immutable-field check and the overlay. Keep the existing `checkImmutableFields(data, client, parentIDs...)` signature (existing tests call it) as a thin wrapper that reads remote and delegates to a new function that accepts the remote object.
- **Rationale**: No extra API call, and the same snapshot is used for the guard and the overlay.
- **Alternatives considered**: Read again inside payload creation: doubles GETs and risks the two reads disagreeing.

## Decision 4: immutable-field guard

- **Decision**: No change to the guard's comparison (remote vs plan/state values). An ignored immutable attribute passes the guard (plan equals remote after refresh, or equals stale state), and the overlay then guarantees the PUT sends the remote value, so the immutable attribute is not changed. Test case added (edge case in spec). With `-refresh=false` and a stale state, the guard may report a mismatch for an ignored immutable attribute; see Open point.

## Decision 5: nothing left to change

- **Decision**: Terraform core does not call Update when the plan has no diff, which satisfies FR-006 when everything is ignored. Add a test covering "only ignored attributes differ".

## Open point (not blocking)

With `-refresh=false`, a stale state can make an ignored immutable attribute differ from the remote value at the guard. Resolution in the design: the guard skips properties detected as ignored. This needs the ignored-path list to be computed before the guard, so detection runs first in `update()`. Recorded as a task.

## Quality-gate baseline (recorded 2026-11-26, before any code change)

Run from the scratchpad with Go 1.26.0 (`go vet`, `golint -set_exit_status`, latest `gosec`, which is newer than the Makefile's pinned v2.3.0, so extra rules apply). `go vet` and `golint` are clean. `gosec` reports these pre-existing findings; after the change there must be no others:

| Scope | Location | Rule |
|---|---|---|
| `openapi/...` | `openapi/openapi_v2_security.go:8` | G101 potential hardcoded credentials (constant name) |
| `openapi/...` | `openapi/plugin_config.go:98` | G706 log injection (`log.Printf`) |
| `tests/...` | `main.go:72` (reported twice) | G706 log injection (`log.Println`) |

## Unknowns resolved

No NEEDS CLARIFICATION items remain. Risk to verify with the gray-box e2e test: that Terraform core with SDK v2.16.0 exposes the prior value in `GetRawPlan` during Update for ignored attributes.
