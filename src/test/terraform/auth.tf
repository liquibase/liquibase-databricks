variable "DBX_HOST" {
  type = string
}

variable "DBX_TOKEN" {
  type = string
}

variable "client_id" {
  type = string
}

variable "client_secret" {
  type = string
}

# Initialize the Databricks Terraform provider.
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

## Use environment variables for Github actions
provider "databricks" {
  host          = var.DBX_HOST
  token         = var.DBX_TOKEN
  client_id     = var.client_id
  client_secret = var.client_secret
}

