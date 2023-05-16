variable "TEST_SCHEMA" {
  description = "Name of Liquibase test harness database"
  type = string
  default = "lb_test_harness"
}


variable "TEST_CATALOG" {
  description = "Catalog of liquibase testing database"
  type = string
  default = "main"
}

variable "schema_force_destroy" {
  type = bool
  default = true
}

resource "databricks_schema" "test_harness" {
  catalog_name = var.TEST_CATALOG
  name = var.TEST_SCHEMA
  comment = "This database is for liquibase test harness"
  force_destroy = var.schema_force_destroy
  properties = {
    purpose = "testing"
  }
}