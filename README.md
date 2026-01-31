# de_glassdoor_jobs_pipeline

# Initial Flow Idea

- (All orchestrated and run via Airflow) - Glassdoor webscraping with playwright -> load to gcp gcs, bigquery -> run dbt in Airflow workers using Cosmos + startup shell scripts in venv -> set up dashboards in Looker Studio
- Set up GCP services via Terraform
- Airflow runs via Docker containers. No local set up. Just one instance that writes to GCP.
