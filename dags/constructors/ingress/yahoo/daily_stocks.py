import asyncio

import pandas
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from apps import af_utils, gcp_utils
from apps.file_loader import BigQueryFileLoader
from apps.webscraper_utils import YahooFinanceScraper
from apps.data_source_utils.yahoo_finance_config import SP_500_CONFIG


@dag(
    dag_id=af_utils.get_dag_name(__file__),
    default_args=af_utils.get_default_args(),
    schedule_interval=None,
    catchup=False
)
def dag():

    @task
    def scrape_daily_stocks():
        curr_timestamp = af_utils.get_current_timestamp_str()

        async def run_scraper():
            async with YahooFinanceScraper() as scraper:
                company_symbols = [config["symbol"] for config in SP_500_CONFIG]
                return await scraper.scrape_companies_data(company_symbols=company_symbols, stock_or_profile="stock", max_concurrency=10)

        all_data = asyncio.run(run_scraper())
        df = pandas.DataFrame(all_data)
        df.to_csv(f"/tmp/daily_stocks_{curr_timestamp}.csv", index=False)
        gcp_utils.upload_to_gcs(
            bucket_name=Variable.get("ingress_bucket"),
            source_file_path=f"/tmp/daily_stocks_{curr_timestamp}.csv",
            destination_blob_name=f"data_sources/yahoo/stocks/daily_stocks_{curr_timestamp}.csv"
        )

    with TaskGroup("ingest_data") as ingest_data:
        file_loader = BigQueryFileLoader()
        file_loader.set_parameters(
            table_dataset_id="yahoo",
            table_id="stocks",
            operation="merge",
            primary_keys=["company_symbol", "load_datetime"],
            rows_to_skip=1
        )
        file_loader.build_dag()

    scrape_daily_stocks() >> ingest_data


dag()
