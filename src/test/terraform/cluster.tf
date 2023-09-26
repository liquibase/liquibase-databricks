variable "cluster_name" {
  description = "A name for the test cluster"
  type = string
  default = "LiquibaseTestCluster"
}
variable "cluster_autotermination_minutes" {
  description = "How many mins before terminating"
  type = number
  default = 30
}
variable "cluster_num_workers" {
  description = "Number of workers for the cluster"
  type = number
  default = 1
}
variable "cluster_data_security_mode" {}


variable "custom_spark_version" {
  description = "A custom spark version for an image"
  type = string
  default = "13.3.x-scala2.12"
}

# Create the cluster with the "smallest" amount
# of resources allowed.
data "databricks_node_type" "smallest" {
  local_disk = true
  min_cores = 4
  photon_worker_capable = true
  photon_driver_capable = true
}

# Use the latest Databricks Runtime
# Long Term Support (LTS) version.
data "databricks_spark_version" "photon" {
  long_term_support = true
  photon = true
}

resource "databricks_cluster" "this" {
  cluster_name            = var.cluster_name
  node_type_id            = data.databricks_node_type.smallest.id
  spark_version           = var.custom_spark_version
  autotermination_minutes = var.cluster_autotermination_minutes
  num_workers             = var.cluster_num_workers
  data_security_mode      = var.cluster_data_security_mode
}

output "cluster_url" {
  value = databricks_cluster.this.id
}