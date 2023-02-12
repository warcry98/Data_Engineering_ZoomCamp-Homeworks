locals {
  data_lake_bucket = "prefect-de-zoomcamp"
  local_data       = jsondecode(file("${path.module}/local-values.json"))
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-central1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}

variable "BQ_TABLE1" {
  description = "BigQuery Dataset Table that raw data will be written to"
  type        = string
  default     = "data_trip"
}

variable "BQ_TABLE2" {
  description = "BigQuery Dataset External Table that raw data (from GCS) will be written to"
  type        = string
  default     = "fvh_trip"
}
