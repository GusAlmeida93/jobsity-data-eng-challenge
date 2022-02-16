terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.9.0"
    }
  }
}

provider "google" {
  credentials = file("cred.json")

  project = "jobsity-data-eng-challenge-dev"
  region  = "us-east1"
  zone    = "us-east1-b"
}


resource "google_project_iam_custom_role" "jobsity-airflow" {
  role_id     = "jobsity_airflow"
  title       = "Jobsity Airflow"
  description = "Role for Airflow Permissions"
  permissions = var.airflow_custom_role
}

resource "google_project_iam_custom_role" "jobsity-bigquery" {
  role_id     = "jobsity_biquery"
  title       = "Jobsity BigQuery User"
  description = "Role for BigQuery User Permissions"
  permissions = var.bigquery_user_custom_role
}