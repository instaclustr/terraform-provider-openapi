# Quickstart: validating INS-45117

1. Unit tests: `go test ./openapi/ -run 'Update|IgnoreChanges|Payload' -count=1`
2. Gray-box e2e: `go test ./tests/e2e/ -run IgnoreChanges -count=1` (the new test uses the existing in-process provider and a stub API, and asserts the PUT body).
3. Manual check against a stub or sandbox API:
   - Create a resource, then edit config so attribute A (ignored) has a new value and B (not ignored) changes.
   - `terraform plan` shows only B. `terraform apply` succeeds and the PUT body contains the prior value of A.
   - Remove A from `ignore_changes`; the next plan shows A and apply sends the new value.
4. Console drift: in the stub API change the ignored attribute's value after the last apply, then run `terraform apply -refresh=false` with another attribute changed. The PUT body must carry the stub's current value for the ignored attribute (FR-010).
5. Regression: `go vet`, `golint`, `go test` for all packages except `tests/integration`, and `go test ./tests/e2e/` must pass with no changes to existing tests. `make test` is not suitable on machines with a newer Go than the project's: `make fmt` reformats unrelated files and `make lint` runs `go get -u`. Run the steps individually (see tasks.md tooling note).

## Manual validation recipe (used for T029)

Build the provider (`go build -o <dir>/terraform-provider-openapi .`), point Terraform at it with a CLI config that has `dev_overrides { "registry.terraform.io/hashicorp/openapi" = "<dir>" }`, export `OTF_VAR_openapi_SWAGGER_URL` to a stub API that serves `tests/e2e/data/gray_box_test_data/ignore-changes/openapi.yaml` (host filled in) and logs every PUT body. Then:

1. `terraform apply` with `label = "a"`, `node_size = "small"`.
2. Change `node_size` on the stub (the "console" edit) to `xlarge`.
3. Set `label = "b"`, `node_size = "large"` plus `lifecycle { ignore_changes = [node_size] }`.
4. `terraform plan` shows only `label`. `terraform apply -refresh=false` sends `{"label":"b","node_size":"xlarge"}`.
5. Remove the `lifecycle` block: the plan shows `node_size` and the next apply sends `large`.
6. Only `node_size` differing in the config (still ignored): `No changes`, no PUT.

Expected with the unfixed provider for step 4: `{"label":"b","node_size":"large"}` (the console value is overwritten).
