# node_size is ignored: the HCL asks for "large" but Terraform must keep what the API holds. Only label changes.
resource "openapi_cdn_v1" "my_cdn" {
  label     = "b"
  node_size = "large"
  lifecycle {
    ignore_changes = [node_size]
  }
}
