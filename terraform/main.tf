terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}
provider "google" {
  project     = var.project_id
  region      = var.region
}



# --- STORAGE ---

# Create GCS ingress bucket
resource "google_storage_bucket" "ingress" {
  name          = "${var.ingress_bucket_name}"
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"
  public_access_prevention = "enforced"
}

# Create GCS archive bucket
resource "google_storage_bucket" "archive" {
  name          = "${var.archive_bucket_name}"
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"
  public_access_prevention = "enforced"
}


# --- BIGQUERY ---

# Enable BigQuery API for project
resource "google_project_service" "bigquery_api" {
  service = "bigquery.googleapis.com"
  disable_on_destroy = false
}

# Create ingress BigQuery dataset (schema)
resource "google_bigquery_dataset" "ingress_datasets" {

  for_each                    = toset(var.data_sources)
  dataset_id                  = "ingress_${each.key}"
  description                 = "Schema for holding ingress tables"
  location                    = var.region
  delete_contents_on_destroy  = true
  depends_on                  = [ google_project_service.bigquery_api ]
}
resource "google_bigquery_dataset" "ods_datasets" {

  for_each                    = toset(var.data_sources)
  dataset_id                  = "ods_${each.key}"
  description                 = "Schema for holding ods tables"
  location                    = var.region
  delete_contents_on_destroy  = true
  depends_on                  = [ google_project_service.bigquery_api ]
}
