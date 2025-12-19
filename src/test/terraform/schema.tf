variable "TF_VAR_TEST_SCHEMA" {
  description = "Name of Liquibase test harness database"
  type        = string
  default     = "liquibase_harness_test_ds"
}


variable "TF_VAR_TEST_CATALOG" {
  description = "Catalog of liquibase testing database"
  type        = string
  default     = "main"
}

variable "schema_force_destroy" {
  type    = bool
  default = true
}

resource "databricks_schema" "test_harness" {
  catalog_name  = var.TF_VAR_TEST_CATALOG
  name          = var.TF_VAR_TEST_SCHEMA
  comment       = "This database is for liquibase test harness"
  force_destroy = var.schema_force_destroy
  properties = {
    purpose = "testing"
  }
}
