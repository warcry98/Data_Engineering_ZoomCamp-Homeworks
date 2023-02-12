terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project     = local.local_data["project_id"]
  region      = var.region
  credentials = file(local.local_data["credential_file"])
}

resource "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.data_lake_bucket}_${local.local_data["project_id"]}" # Concatenating DL bucket & Project name for unique naming
  location = var.region

  # Optional, but recommended settings:
  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.BQ_DATASET
  project                    = local.local_data["project_id"]
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "dataset_table1" {
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = var.BQ_TABLE1
  deletion_protection = false
}

resource "google_bigquery_table" "dataset_table2" {
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = var.BQ_TABLE2
  deletion_protection = false
}
