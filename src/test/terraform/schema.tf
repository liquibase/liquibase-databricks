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

# Data source to retrieve information about the currently authenticated user or service principal
# This will automatically detect whether authentication is via user account or service principal
data "databricks_current_user" "me" {}

resource "databricks_schema" "test_harness" {
  catalog_name  = var.TF_VAR_TEST_CATALOG
  name          = var.TF_VAR_TEST_SCHEMA
  comment       = "This database is for liquibase test harness"
  force_destroy = var.schema_force_destroy
  properties = {
    purpose = "testing"
  }
}

# Grant specific schema-level permissions to current user/service principal
# These permissions provide comprehensive management capabilities for the test schema:
# - USE_SCHEMA: Allows viewing and using the schema
# - CREATE_TABLE: Allows creating new tables within the schema
# - MODIFY: Allows modifying existing tables and their data
# - SELECT: Allows reading data from tables in the schema
resource "databricks_grant" "schema_manage" {
  schema = databricks_schema.test_harness.id

  principal  = data.databricks_current_user.me.user_name
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
}

# Alternative grant providing ALL_PRIVILEGES (equivalent to MANAGE permission)
# This grants the most comprehensive permission set possible on the schema
# Use this if you need full administrative control over the schema
# Note: You may choose to use either this grant OR the specific permissions above
resource "databricks_grant" "schema_full_manage" {
  schema = databricks_schema.test_harness.id

  principal  = data.databricks_current_user.me.user_name
  privileges = ["ALL_PRIVILEGES"]
}