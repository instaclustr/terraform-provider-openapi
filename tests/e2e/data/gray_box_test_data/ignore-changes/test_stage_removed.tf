# the ignore_changes block is gone: node_size is managed by Terraform again
resource "openapi_cdn_v1" "my_cdn" {
  label     = "b"
  node_size = "large"
}
