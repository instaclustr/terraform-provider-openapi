# ignore_changes = all: Terraform keeps the whole prior state, so nothing is updated even though the HCL changed
resource "openapi_cdn_v1" "my_cdn" {
  label     = "b"
  node_size = "large"
  lifecycle {
    ignore_changes = all
  }
}
