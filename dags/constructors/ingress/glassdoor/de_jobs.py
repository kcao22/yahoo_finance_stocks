import asyncio
from airflow.sdk import dag, task
import requests

from apps import af_utils
from apps.webscraper_utils import GlassdoorScraper


@dag(
    dag_id=af_utils.get_dag_name(__file__),
    default_args=af_utils.get_default_args(),
    schedule=None,
    catchup=False
)
def dag():

    # @task
    # def run_scraper():
    #     async def execute_scrape():
    #         async with GlassdoorScraper() as scraper:
    #             return await scraper.scrape_job_listings()
    #     results = asyncio.run(execute_scrape())
        
    #     print(f"Scraped {len(results) if results else 0} jobs.")
    #     return results

    # run_scraper()
    @task
    def test_glassdoor_api():
        url = "https://glassdoor-real-time.p.rapidapi.com/jobs/search"
        querystring = {
            "query": "Data Engineer",
            "datePosted": 1
        }
        headers = {"x-rapidapi-host": "glassdoor-real-time.p.rapidapi.com"}
        response = requests.get(url, headers=headers, params=querystring)
        print(response.json())

    test_glassdoor_api()


dag()
