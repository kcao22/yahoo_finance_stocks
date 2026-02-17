import asyncio

import pandas
from airflow.sdk import dag, task

from apps import af_utils
from apps.webscraper_utils import YahooFinanceScraper
from apps.data_source_utils.yahoo_finance_config import DAILY_EXTRACT_CONFIG


@dag(
    dag_id=af_utils.get_dag_name(__file__),
    default_args=af_utils.get_default_args(),
    schedule=None,
    catchup=False
)
def dag():

    @task
    def scrape_stocks():
        curr_timestamp = af_utils.get_current_timestamp_str()
        scraper = YahooFinanceScraper()
        company_symbols = [config["symbol"] for config in DAILY_EXTRACT_CONFIG]
        all_data = scraper.scrape_companies_data(
            company_symbols=company_symbols
        )
        df = pandas.DataFrame(all_data)
        df.to_csv(f"/tmp/daily_stocks_{curr_timestamp}.csv", index=False)
    
    scrape_stocks()


dag()
