# Yahoo Finance Stocks ELT Pipeline - Tracking Daily Stock Data and Market Profiles

**ELT Pipeline Process**:
![Alt Text](https://github.com/kcao22/yahoo_finance_stocks/blob/main/docs/yahoo_finance_stocks_elt.png)

**Dashboard**:
![Alt Text](https://github.com/kcao22/yahoo_finance_stocks/blob/main/docs/)

**Dimensional Model Design**:
![Alt Text](https://github.com/kcao22/yahoo_finance_stocks/blob/main/docs/erd_diagram.png)

## Source Data
 - Yahoo Finance (Scraped via Playwright Stealth)
 - S&P 500 company configurations

## Project Goals
 - Stand up modern data engineering stack pipelines using tools like infrastructure-as-code for Google Cloud Platform (IaC), Docker containerization for running services, Python object oriented programming for extract and load, dbt for transforms, and continuous integration (CI).
 - Set up basic analysis of stock data scraped from Yahoo Finance to gain a better understanding of stocks as a novice in knowledge pertaining to stocks.

## Libraries and Resources Used
 - **Python Version**: 3.11
 - **Main Packages**: Apache Airflow, Docker, GCP, Playwright, Pandas, dbt-bigquery, Pytest
 - **Resources**: Google Cloud Platform (GCS, BigQuery), Terraform, Docker

# Pipeline Architecture and Process

## Infrastructure as Code - Terraform and GCP
 1. Utilized Terraform to stand up the entire cloud infrastructure on GCP. This includes the automated provisioning of GCS buckets for landing and archiving, BigQuery datasets for different data layers, and specific IAM roles to adhere to the principle of least privilege.
 2. Using IaC ensures that the environment is reproducible and version controlled. I can further use IaC to tear down my cloud infrastructure after project completion.


## Data Extraction and Ingestion - Playwright Scraper and GCP Integration
 1. Developed a `YahooFinanceScraper` class utilizing `playwright-stealth` to programmatically extract daily stock data and company profiles from Yahoo Finance for 100 companies (chosen arbitrarily at one point via the "top performing" page). The implementation leverages asynchronous logic for scalable scraping / extraction and incorporates randomized user agent selection to mitigate bot detection.
 2. I also implemented a `BigQueryFileLoader` class to manage the data lifecycle from GCS (CSV loaded from web scraping task) to BigQuery. This class handles archiving historical files and loading fresh data into ingress tables before promoting it to the Operational Data Store (ODS) layer.

## Data Warehouse - GCS to BigQuery
 1. Raw data is landed in a GCS ingress bucket.
 2. The `BigQueryFileLoader` orchestrates the movement of this data into BigQuery. The process involves truncating ingress tables and performing either `merge`, `append`, or `replace` operations into the ODS layer based on the dataset parameters.

## Data Transformation and Modeling - dbt
 1. **dbt** is used as the transformation tool for this project (personal interest in setting up dbt infra). I designed a star schema dimensional model consisting of fact and dimension tables to support slicing into daily stock performance based on different metrics like a company's sector, industry, etc.
 2. The dbt structure includes an `dbt_intermediate` layer for housing lightly cleaned models, and `dbt_marts` for the final star schem tables. dbt tests are also implemented to ensure transformation of data from the ODS layer to the mart layer is behaving as expected (e.g. unique keys). 

## Orchestration - Apache Airflow
 1. Apache Airflow serves as the primary orchestrator from extract to load to transform.
 2. Airflow services are all instantiated and running via Docker containers as set up by Docker images the project's docker/ folder.

 ## Continuous Integration
 1. Configured unit tests to test portions of code in isolation. Testing full data pipelines is brittle, and integrating tests for critical portions of implemented code can serve as an efficient means of verifying code behavior.
 2. Implemented GitHub Actions workflow that utilizes the GitHub Container Registry (**GHCR**) to cache CI environment for optimal build / test.
 3. Unit tests are implemented using `pytest` to validate DAG logic, utility functions, and task generation within the `BigQueryFileLoader`.
