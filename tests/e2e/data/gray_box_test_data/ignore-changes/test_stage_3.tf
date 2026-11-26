# node_size was changed outside Terraform (the test changes it on the API side before this step) and is still ignored.
resource "openapi_cdn_v1" "my_cdn" {
  label     = "c"
  node_size = "large"
  lifecycle {
    ignore_changes = [node_size]
  }
}
