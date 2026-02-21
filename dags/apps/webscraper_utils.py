import asyncio
import random

from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from apps.data_source_utils.yahoo_finance_config import DAILY_EXTRACT_CONFIG, DIM_DATA_EXTRACT_CONFIG

class WebScraper:
    def __init__(self, websocket_endpoint: str = "ws://playwright-browser:3000/"):
        self.websocket_endpoint = websocket_endpoint  # Websocket = two-way tunnel to containerized playwright browser
        self.playwright_context_manager = None
        self.playwright = None
        self.browser = None
        self.context = None

    async def __aenter__(self):
        """
        Method for async with to understand how 
        """
        self.playwright_context_manager = Stealth().use_async(async_playwright())
        self.playwright = await self.playwright_context_manager.__aenter__() 
        # Connect to the running containerized playwright service
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", 
                "--disable-setuid-sandbox", 
                "--disable-dev-shm-usage"
            ]
        )        
        # Set user agent and viewport settings in new context session
        self.context = await self.browser.new_context(
            user_agent=self._get_random_user_agent(),
            viewport={"width": 1920, "height": 1080}
        )
        self.page = await self.context.new_page()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    def _get_random_user_agent(self) -> str:
        user_agents = [
            # Chrome (Windows)
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            # Firefox (Windows)
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            # Edge (Windows)
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
        ]
        return random.choice(user_agents)

    async def click_button(self, page, button_selector, selector_desc: str, state: str = "visible", timeout: int = 10000):
        """
        General method for clicking a button and waiting for the resulting page changes to load.
        """
        # Set CSS path to modal button
        try:
            await page.wait_for_selector(button_selector, state=state, timeout=timeout)
            print(f"{selector_desc} detected. Clicking...")
            # Click selector button
            await page.click(button_selector)
        except:
            print(f"No {selector_desc} detected. Continuing with scraping...")
            pass

    async def locate_text(self, page, locator_class: str, locator_desc: str) -> str:
        """
        Finds element of a page based on CSS selector and returns inner text.
        :param page: Playwright page object to perform actions on
        :param locator_class: CSS selector to locate element of interest
        :param locator_desc: Description of element being located for logging purposes
        :return: Inner text of located element or "N/A" if element not found
        """
        locator = page.locator(locator_class)
        if await locator.count() > 0:
            text = await locator.first.inner_text()
            print(f"{locator_desc}: {text}")
            return text
        else:
            print(f"No {locator_desc} detected.")
            return "N/A"


class YahooFinanceScraper(WebScraper):
    def __init__(self, websocket_endpoint: str = "ws://playwright-browser:3000/"):
        super().__init__(websocket_endpoint)
        self.base_url = "https://finance.yahoo.com/quote"

    async def scrape_company_data(self, company_symbol: str, extract_config: list[dict]) -> list[dict]:
        """
        Scrapes a single company's stock data from Yahoo Finance given a company symbol (e.g. AAPL for Apple, MSFT for Microsoft).
        Iterates over DAILY_EXTRACT_CONFIG to extract relevant stock data points based on provided CSS selectors and returns a dictionary of extracted data.
        :param company_symbol: Stock ticker symbol for company of interest.
        :param extract_config: The config list of dictionaries outlining target field and corresponding CSS selector for extraction.
        :return: Dictionary of extracted stock data for given company
        """
        # Launch with context to use specific user agent settings / viewport settings
        # Browser is also heavier / more resource intensive
        page = await self.context.new_page()
        # Proceed once basic HTML loads
        extract_url = f"{self.base_url}/{company_symbol}" if extract_config == DAILY_EXTRACT_CONFIG else f"{self.base_url}/{company_symbol}/profile"
        print(f"Navigating to {extract_url}...")
        await page.goto(extract_url, wait_until="domcontentloaded", timeout=60000)
        if extract_url.endswith("/profile"):
            try:
                await page.wait_for_selector("a[href*='/sectors/']", timeout=5000)
            except:
                print(f"Timed out waiting for profile links for {company_symbol}")
        data = {}
        for extract_mappings in extract_config:
            # Extract data from page
            data_value = await self.locate_text(
                page=page,
                locator_class=extract_mappings["selector_field"],
                locator_desc=extract_mappings["locator_desc"]
            )
            data[extract_mappings["target_field"]] = data_value
        return data

    async def scrape_companies_data(self, company_symbols: list[str], stock_or_profile: str, max_concurrency: int = 10) -> list[dict]:
        """
        Scrapes companies data for multiple companies concurrently from Yahoo Finance.
        :param company_symbols: List of stock ticker symbols for companies of interest
        :param stock_or_profile: Whether to extract stock data or profile data for the companies. Determines which config to use for extraction.
        :param max_concurrency: Maximum number of concurrent scraping tasks
        :return: List of dictionaries containing extracted stock data for each company
        """
        if stock_or_profile not in ["stock", "profile"]:
            raise ValueError(f"Invalid stock_or_profile value: {stock_or_profile}. Must be 'stock' or 'profile'.")
        if stock_or_profile == "stock":
            extract_config = DAILY_EXTRACT_CONFIG
        else:
            extract_config = DIM_DATA_EXTRACT_CONFIG
        semaphore = asyncio.Semaphore(max_concurrency)

        async def sem_task(symbol):
            async with semaphore:
                return await self.scrape_company_data(company_symbol=symbol, extract_config=extract_config)

        tasks = [sem_task(symbol) for symbol in company_symbols]
        return await asyncio.gather(*tasks)
