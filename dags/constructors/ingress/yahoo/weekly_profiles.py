import asyncio

import pandas
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from apps import af_utils, gcp_utils
from apps.data_source_utils.yahoo_finance_config import SP_500_CONFIG
from apps.file_loader import BigQueryFileLoader
from apps.webscraper_utils import YahooFinanceScraper


@dag(
    dag_id=af_utils.get_dag_name(__file__),
    default_args=af_utils.get_default_args(),
    start_date=pendulum.datetime(2026, 3, 2, tz="US/Pacific"),
    schedule_interval="0 19 * * 6",  # Saturday 7 PM
    catchup=False,
    params={
        "symbols": Param(
            default="",
            type=["string", "null"],
            description="Comma-separated list of company symbols to scrape.",
        )
    },
)
def dag():

    @task
    def scrape_weekly_profiles():
        curr_timestamp = af_utils.get_current_timestamp_str()

        async def run_scraper():
            async with YahooFinanceScraper() as scraper:
                company_symbols = []
                context = get_current_context()
                if context["params"]["symbols"]:
                    company_symbols = [
                        symbol.strip()
                        for symbol in context["params"]["symbols"].split(",")
                    ]
                else:
                    company_symbols = [config["symbol"] for config in SP_500_CONFIG]
                return await scraper.scrape_companies_data(
                    company_symbols=company_symbols,
                    stock_or_profile="profile",
                    max_concurrency=25,
                )

        all_data = asyncio.run(run_scraper())
        df = pandas.DataFrame(all_data)
        df.to_csv(f"/tmp/weekly_profiles_{curr_timestamp}.csv", index=False)
        gcp_utils.upload_to_gcs(
            bucket_name=Variable.get("ingress_bucket"),
            source_file_path=f"/tmp/weekly_profiles_{curr_timestamp}.csv",
            destination_blob_name=f"data_sources/yahoo/profiles/weekly_profiles_{curr_timestamp}.csv",
        )

    with TaskGroup("ingest_data") as ingest_data:
        file_loader = BigQueryFileLoader()
        file_loader.set_parameters(
            table_dataset_id="yahoo",
            table_id="profiles",
            operation="merge",
            primary_keys=["company_symbol", "load_date"],
            rows_to_skip=1,
        )
        file_loader.build_dag()

    scrape_weekly_profiles() >> ingest_data


dag()
