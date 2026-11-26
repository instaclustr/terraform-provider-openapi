# Data Model: INS-45117

No persisted data changes. Concepts used by the design:

| Concept | Source | Notes |
|---|---|---|
| Config value | `ResourceData.GetRawConfig()` | User's HCL. Never altered by `ignore_changes`. |
| Plan value | `ResourceData.GetRawPlan()` | Core replaces ignored attributes with prior state. May contain unknown values for computed attributes. |
| Remote object | `readRemote()` (one `GET` before the PUT) | JSON-decoded `map[string]interface{}` in API property names. The source of truth for ignored values (FR-010). |
| Ignored path | derived | Attribute path (Terraform names, list indexes) where plan is known and non-null and plan != config, and either config is known and non-null, or config is null and the attribute is not computed. |
| Update payload | `createPayloadFromTerraformConfig` output, then overlay | Config values everywhere, except ignored paths take the remote value (fallback: plan value). |

## Rules

- Read-only properties never enter the payload (unchanged).
- Unknown plan values never mark a path as ignored.
- Null config value: ignored only when the attribute is not computed and the plan value is known and non-null (for a non-computed attribute this combination can only come from `ignore_changes`); for computed attributes behaviour is unchanged (null config with a prior-state plan value is normal there).
- Detection compares `cty.Value`s (`IsNull`, `IsKnown`, `RawEquals`) so null and unknown are never confused with zero values.
- Overlay writes use API property names, translated from Terraform names through the resource schema.
- A remote value that is missing at an ignored path (for example write-only or sensitive properties) falls back to the plan value.
- Lists: per-index overlay only when plan and config lengths match and the property is not `x-terraform-ignore-order`; otherwise the whole list is replaced by the remote list.
- With no ignored paths the payload is identical to today's (FR-007).
