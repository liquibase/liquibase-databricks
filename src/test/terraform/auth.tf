variable "databricks_connection_profile" {
  description = "The name of the Databricks connection profile to use."
  type        = string
}

# Initialize the Databricks Terraform provider.
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# Use Databricks CLI authentication.
provider "databricks" {
  profile = var.databricks_connection_profile
}

# Retrieve information about the current user.
data "databricks_current_user" "me" {}