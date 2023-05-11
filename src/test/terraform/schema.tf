variable "testing_schema" {
  description = "Name of Liquibase test harness database"
  type = string
  default = "lb_test_harness"
}


variable "testing_catalog" {
  description = "Catalog of liquibase testing database"
  type = string
  default = "liquibase"
}

resource "databricks_schema" "test_harness" {
  catalog_name = var.testing_catalog
  name = var.testing_schema
  comment = "This database is for liquibase test harness"
  properties = {
    purpose = "testing"
  }
}