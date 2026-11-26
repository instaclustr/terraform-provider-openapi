# Contract: update payload value selection

The provider exposes no new HCL syntax or API. The contract is the observable behaviour of `PUT` during apply.

For each writable property P of a resource being updated, with `Remote(P)` the value read from the API immediately before the update:

| Config(P) | Plan(P) | Value sent in PUT |
|---|---|---|
| set | equal to config | Config(P) (unchanged behaviour) |
| set | known, different from config (ignore_changes) | Remote(P); if the API returned none, Plan(P) |
| set | unknown | Config(P) |
| null | known, non-null, attribute not computed (ignore_changes on an omitted attribute) | Remote(P); if the API returned none, Plan(P) |
| null | any other | unchanged behaviour |

Invariants:
- Every attribute shown as unchanged in the plan is sent with the API's current value, so a change made outside Terraform is preserved (FR-003, FR-010), including with `-refresh=false`.
- Every attribute shown as changed is sent with its planned value.
- The `GET` used for the immutable-field check and the `GET` used for the overlay are the same call.
