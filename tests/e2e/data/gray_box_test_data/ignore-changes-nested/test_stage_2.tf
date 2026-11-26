# settings.size is ignored while label and settings.region are updated
resource "openapi_cdn_v1" "my_cdn" {
  label = "b"
  settings {
    size   = "large"
    region = "eu"
  }
  lifecycle {
    ignore_changes = [settings[0].size]
  }
}
