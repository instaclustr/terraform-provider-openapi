# Feature Specification: Respect lifecycle ignore_changes on Update

**Feature Branch**: `feature/instaclustr-speckit-releases`

**Created**: 2026-11-25

**Status**: Draft

**Input**: User description: "Customers need lifecycle { ignore_changes = [...] } to ignore drift on some attributes and still apply other changes. DoorDash hit this again while updating Kafka clusters. Plan hid the ignored fields, then apply sent them anyway and CMAPI rejected the update. Reference - https://instaclustr.atlassian.net/browse/INS-45117"

## Clarifications

### Session 2026-11-25

- Q: When someone changes an ignored attribute outside Terraform (for example firewall rules in the console) and Terraform refreshes first, should apply keep the console's current value for that attribute? → A: Yes. Apply sends the refreshed (console) value, so the console change is preserved.

### Session 2026-11-26

- Q: If a customer runs plan or apply with `-refresh=false`, so Terraform's state may be out of date, should apply still send the (possibly stale) state value for an ignored attribute? → A: No. Take the current value from the API read the provider already makes before the update, so console changes survive even without a refresh.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Update a resource while ignoring drifted attributes (Priority: P1)

A customer manages a cluster with Terraform and lists some attributes in `lifecycle { ignore_changes = [...] }` because those values were changed outside Terraform or must not be managed by it. They then change a different, non-ignored attribute (for example, node size or a Kafka setting) and run plan and apply. The update must succeed and contain only what the customer asked to change.

**Why this priority**: This is the reported failure. Customers (e.g. DoorDash with Kafka clusters) are blocked from updating clusters at all because the update is rejected by the Instaclustr API.

**Independent Test**: Create a resource, mark an attribute as ignored, change that attribute's configured value and a second, non-ignored attribute, then plan and apply. The apply succeeds and the update request leaves the ignored attribute out or unchanged from its current remote value.

**Acceptance Scenarios**:

1. **Given** a managed resource with attribute A in `ignore_changes` and the configured value of A differing from the remote value, **When** the customer changes non-ignored attribute B and applies, **Then** the plan shows only B changing and the apply updates B successfully.
2. **Given** the same setup, **When** the update is sent to the API, **Then** the update does not attempt to change A, so the API does not reject it for an unsupported or invalid change to A.
3. **Given** attribute A is ignored and nothing else changed, **When** the customer runs plan, **Then** no changes are reported.
4. **Given** attribute A is ignored and someone changed A outside Terraform (for example firewall rules edited in the Instaclustr console), **When** the customer changes non-ignored attribute B and applies, **Then** the console's value of A is preserved and B is updated.
5. **Given** the same outside change but the customer runs the apply with refresh disabled (`-refresh=false`), **When** the update is sent, **Then** A still keeps the value the API currently holds, not the older value stored in state.
6. **Given** attribute A is in `ignore_changes` but is not set in the configuration, **When** the customer changes non-ignored attribute B and applies, **Then** A keeps its current API value and is not sent as empty or zero.

---

### User Story 2 - Plan and apply stay consistent (Priority: P1)

The customer must be able to trust that what the plan displays is what apply performs. If the plan hides a field because it is ignored, apply must not send a different value for it.

**Why this priority**: The mismatch between plan (field hidden) and apply (field sent anyway) is the root of the incident and erodes trust in the tool.

**Independent Test**: Compare the set of attribute changes shown in the plan against the set of changes sent during apply for a resource with ignored attributes; they must match.

**Acceptance Scenarios**:

1. **Given** a resource with ignored attributes, **When** the customer applies a plan, **Then** no ignored attribute value is changed by the apply compared to the resource's current remote value.
2. **Given** an ignored attribute is nested within a complex object or list, **When** a sibling attribute changes, **Then** only the sibling change is applied and the ignored nested attribute is left as-is.

---

### User Story 3 - Non-ignored behavior is unchanged (Priority: P2)

Customers who do not use `ignore_changes` see no behavior change, and removing an attribute from `ignore_changes` makes Terraform manage it again.

**Why this priority**: Prevents regressions for all existing users.

**Independent Test**: Run existing update scenarios without `ignore_changes`; results are identical to before. Remove an attribute from `ignore_changes` and confirm the next plan shows its drift and apply reconciles it.

**Acceptance Scenarios**:

1. **Given** a resource with no `ignore_changes`, **When** the customer updates any attribute, **Then** behavior is identical to the current release.
2. **Given** attribute A was previously ignored, **When** it is removed from `ignore_changes`, **Then** the next plan shows A's difference and apply updates it.

---

### Edge Cases

- All changed attributes are ignored: no update request is made and apply reports no changes.
- Ignored attribute is nested inside a list or object that is otherwise changing.
- Ignored attribute is required by the API on every update: its current remote value is used rather than the configured one.
- `ignore_changes = all` is specified.
- Ignored attribute is immutable on the API side and its configured value differs from the remote value: the update is not aborted by the immutable-field check, and the immutable attribute is sent with its current remote value, never the configured one.
- Refresh disabled (`-refresh=false`) and an ignored attribute was changed outside Terraform since the last refresh: apply still sends the value the API currently holds.
- Ignored attribute is omitted from the configuration (for example removed from the HCL): apply sends the API's current value, not an empty or zero value.
- Ignored attribute was changed outside Terraform (for example firewall rules edited in the console) and the configuration still holds the old value: apply keeps the console value.
- The API still rejects an update for a reason unrelated to ignored attributes: the API error is surfaced unchanged.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The provider MUST honor attributes listed in `lifecycle { ignore_changes }` when applying an update, so that the update does not change them.
- **FR-002**: The values applied for an update MUST be consistent with what the plan displayed; attributes hidden from the plan as ignored MUST NOT be altered by apply.
- **FR-003**: For an ignored attribute, the provider MUST use the API's current value rather than the configured value wherever the API requires that attribute to be supplied. When the attribute was changed outside Terraform (for example in the Instaclustr console) and a refresh preceded the apply, that outside change MUST be preserved, not reverted.
- **FR-004**: Non-ignored attributes changed in the same apply MUST still be updated.
- **FR-005**: Ignored attributes nested inside objects and lists MUST be honored, including when sibling attributes change.
- **FR-006**: If, after ignoring, nothing remains to change, the provider MUST NOT issue an update request.
- **FR-007**: Resources and updates without `ignore_changes` MUST behave exactly as before.
- **FR-008**: Removing an attribute from `ignore_changes` MUST cause it to be managed again on the next plan/apply.
- **FR-009**: Errors from the API unrelated to ignored attributes MUST be surfaced to the user unchanged.
- **FR-010**: For an ignored attribute, the value sent MUST be the one the API currently holds as read immediately before the update, so that it is correct even when the customer disabled refresh and the stored state is out of date.
- **FR-011**: An attribute listed in `ignore_changes` MUST also be honored when it is not set in the configuration, provided Terraform treats it as user-managed (not computed by the API); its current API value is sent, never an empty or zero value.

### Key Entities

- **Managed resource**: A cluster or similar infrastructure object defined in Terraform and backed by the Instaclustr API, with attributes that may be configured, remote, or both.
- **Ignored attribute**: An attribute named in the resource's `ignore_changes` list whose configured-versus-remote differences must be neither shown nor applied.
- **Update request**: The set of attribute values sent to the API when applying a change.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In the reported scenario (Kafka cluster with drifted ignored attributes plus a legitimate change), 100% of applies succeed where they previously were rejected.
- **SC-002**: For every tested combination of ignored attributes (top-level, nested, list members, `all`), the changes applied match the changes shown in the plan in 100% of cases.
- **SC-003**: Zero regressions in existing update scenarios for resources that do not use `ignore_changes`.
- **SC-004**: Customers no longer need to remove `ignore_changes` or work around the issue manually to update an affected resource; related support escalations for this issue drop to zero.

## Assumptions

- Applies to all resources generated by the provider, not only Kafka clusters; Kafka is the reported case.
- Terraform core already hides ignored attribute differences in the plan; the provider must make apply consistent with that.
- Using the API's current value (read just before the update) for an ignored attribute is the correct way to satisfy API fields that are mandatory on update, and it stays correct when the stored state is out of date.
- Create operations are out of scope; this concerns updates only.
- Ticket INS-45117 contains additional customer details not reviewed here.
