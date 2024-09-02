resource "databricks_sql_endpoint" "this" {
  name             = "Databricks Liquibase Test Harness Endpoint"
  cluster_size     = "Small"
  max_num_clusters = 1
  warehouse_type   = "PRO"
}

output "endpoint_url" {
  value = databricks_sql_endpoint.this.id
}