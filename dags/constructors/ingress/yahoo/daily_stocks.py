import asyncio

import pandas
from airflow.sdk import dag, task

from apps import af_utils
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
                return await scraper.scrape_companies_data(company_symbols=company_symbols, max_concurrency=10)

        all_data = asyncio.run(run_scraper())
        df = pandas.DataFrame(all_data)
        df.to_csv(f"/tmp/daily_stocks_{curr_timestamp}.csv", index=False)

    scrape_daily_stocks()


dag()
