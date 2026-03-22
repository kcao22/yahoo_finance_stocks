import pytest
from apps.webscraper_utils import WebScraper


@pytest.mark.asyncio
async def test_scraper_init():
    scraper = WebScraper(websocket_endpoint="ws://localhost:3000")
    assert scraper.websocket_endpoint == "ws://localhost:3000"


def test_random_user_agent():
    scraper = WebScraper()
    ua = scraper._get_random_user_agent()
    assert isinstance(ua, str)
    assert len(ua) > 10
