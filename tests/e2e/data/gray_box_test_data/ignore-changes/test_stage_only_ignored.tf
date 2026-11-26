# only the ignored attribute differs from the prior state: the plan must be empty
resource "openapi_cdn_v1" "my_cdn" {
  label     = "a"
  node_size = "large"
  lifecycle {
    ignore_changes = [node_size]
  }
}
