locals {
  deployer_email = "liquibase-terraform"
}

module "databricks_liquibase_test" {
  source                     = "terraform-databricks-partner-modules/databricks"
  version                    = "~> 4.4"
  dataset_id                 = "harness_test_ds"
  dataset_name               = "harness_test_ds"
  description                = "DataSet for liquibase harness tests to be run" # updated the description accordingly
  location                   = "US" # Update location if needed
  delete_contents_on_destroy = true
  dataset_labels             = {}
}