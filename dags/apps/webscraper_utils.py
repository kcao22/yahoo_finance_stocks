import asyncio
import random
import pendulum

from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from apps.data_source_utils.yahoo_finance_config import DAILY_EXTRACT_CONFIG, DIM_DATA_EXTRACT_CONFIG
from apps.print_utils import print_logging_info_decorator

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
        # Randomize user agent to avoid bot detection
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
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"
        ]
        return random.choice(user_agents)

    async def click_button(self, page, button_selector, selector_desc: str, state: str = "visible", timeout: int = 10000):
        """
        General method for clicking a button and waiting for the resulting page changes to load. Originally used this for bypassing pop-ups but new
        Yahoo data source does not need this. Retaining for future uses.
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

    async def _restart_playwright(self):
        """
        Helper method to restart playwright browser.
        """
        if self.browser:
            await self.browser.close()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", 
                "--disable-setuid-sandbox", 
                "--disable-dev-shm-usage"
            ]
        )


class YahooFinanceScraper(WebScraper):
    def __init__(self, websocket_endpoint: str = "ws://playwright-browser:3000/"):
        super().__init__(websocket_endpoint)
        self.base_url = "https://finance.yahoo.com/quote"

    async def scrape_company_data(self, company_symbol: str, extract_config: list[dict], max_retries: int = 5) -> list[dict]:
        """
        Scrapes a single company's stock data from Yahoo Finance given a company symbol (e.g. AAPL for Apple, MSFT for Microsoft).
        Iterates over DAILY_EXTRACT_CONFIG to extract relevant stock data points based on provided CSS selectors and returns a dictionary of extracted data.
        :param company_symbol: Stock ticker symbol for company of interest.
        :param extract_config: The config list of dictionaries outlining target field and corresponding CSS selector for extraction.
        :return: Dictionary of extracted stock data for given company
        """
        extract_url = f"{self.base_url}/{company_symbol}" if extract_config == DAILY_EXTRACT_CONFIG else f"{self.base_url}/{company_symbol}/profile"
        retry_fields = ["open", "previous_close", "volume", "avg_volume", "day_range"] if extract_config == DAILY_EXTRACT_CONFIG else ["company_full_time_employees"]
        for attempt in range(max_retries):
            page = None
            current_context = None
            try:
                # Adding a random stagger to individual scraping to avoid bot detection during semaphore parallelized calls
                random_stagger = random.uniform(0.5, 3.0)
                await asyncio.sleep(random_stagger)
                # Launch with context to use specific user agent settings / viewport settings
                # Browser is also heavier / more resource intensive
                # Creating a new context and page for each scrape attempt
                current_context = await self.browser.new_context(
                    user_agent=self._get_random_user_agent(),
                    viewport={"width": 1920, "height": 1080}
                )
                page = await current_context.new_page()
                print(f"(Attempt {attempt + 1}/{max_retries}) for company {company_symbol}...")
                print(f"Navigating to {extract_url}...")
                # Proceed once basic HTML loads
                await page.goto(extract_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(random.uniform(1, 3))  # Buffer to allow potential re-routing from invalid symbol
                if "lookup" in page.url or "404" in page.url:
                    raise ValueError(f"Invalid Symbol: {company_symbol}")
                if extract_url.endswith("/profile"):
                    try:
                        await page.wait_for_selector("a[href*='/sectors/']", timeout=10000)
                    except Exception as e:
                        print(f"Time out waiting for profile links for {company_symbol}: {e}. Proceeding anyways.")
                elif extract_url.endswith(company_symbol):
                    await page.wait_for_selector("h1", timeout=30000)
                    await asyncio.sleep(random.uniform(1, 5))
                data = {"company_symbol": company_symbol}
                for extract_mappings in extract_config:
                    # Extract data from page
                    data_value = await self.locate_text(
                        page=page,
                        locator_class=extract_mappings["selector_field"],
                        locator_desc=extract_mappings["locator_desc"]
                    )
                    data[extract_mappings["target_field"]] = data_value
                # Retry if crucial daily data is missing
                missing_fields = [field for field in retry_fields if data.get(field) in [None, "N/A", ""]]
                if missing_fields and attempt < max_retries - 1:
                    print(f"Missing crucial fields {missing_fields} for {company_symbol}. Retrying...")
                    raise ValueError(f"Missing crucial data: {missing_fields}")
                await page.close()
                return data
            except Exception as e:
                print(f"Error scraping data for {company_symbol} on attempt {attempt + 1}: {e}")
                if page:
                    await page.close()
                if attempt == max_retries - 1:
                    print(f"Max retries reached for {company_symbol}...")
                    data = {"company_symbol": company_symbol}
                    for extract_mappings in extract_config:
                        data[extract_mappings["target_field"]] = None
                    raise ValueError(f"\n{'*' * 100}\n{company_symbol} failed extraction. Please re-run.\n{'*' * 100}")
                else:
                    print(f"Retrying scrape for {company_symbol}...")
                    await asyncio.sleep(2 ** attempt + 1 + random.uniform(0, 1))  # Exponential backoff before retrying
            finally:  # Ensure page and context are closed regardless of results.
                if page:
                    await page.close()
                if current_context:
                    await current_context.close()

    @print_logging_info_decorator
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
        results = []
        semaphore = asyncio.Semaphore(max_concurrency)
        for i in range(0, len(company_symbols), max_concurrency):
            batch = company_symbols[i: i + max_concurrency]
            print(f"Starting batch {i//max_concurrency + 1}...")
            await self._restart_playwright()
            tasks = [self.sem_task(symbol, semaphore, extract_config) for symbol in batch]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
        return results

    async def sem_task(self, symbol, semaphore, extract_config):
        async with semaphore:
            return await self.scrape_company_data(company_symbol=symbol, extract_config=extract_config)
