resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
}
resource "databricks_sql_endpoint" "this" {
  name             = "Databricks Liquibase Test Harness Endpoint-${random_string.suffix.result}"
  cluster_size     = "Small"
  max_num_clusters = 1
  warehouse_type   = "PRO"
  auto_stop_mins   = 10
  lifecycle {
    create_before_destroy = true
  }
}

output "endpoint_url" {
  value = databricks_sql_endpoint.this.id
}
