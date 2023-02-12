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

source "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
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

resource "google_bigquery_table" "dataset_table" {
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = var.BQ_TABLE
  deletion_protection = false
}
