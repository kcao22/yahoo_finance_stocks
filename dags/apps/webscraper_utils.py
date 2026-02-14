from playwright.async_api import async_playwright


class WebScraper:
    def __init__(self, websocket_endpoint: str = "ws://localhost:3000/"):
        self.websocket_endpoint = websocket_endpoint  # Websocket = two-way tunnel to containerized playwright browser
        self.playwright = None
        self.browser = None
        self.context = None
        
    async def __aenter__(self):
        """
        Method for async with to understand how 
        """
        # Initialize plawywright
        self.playwright = await async_playwright().start()
        # Connect to the running containerized playwright service
        self.browser = await self.playwright.chromium.connect(self.websocket_endpoint)
        # Set user agent and viewport settings in new context session
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        return self

    async def __aexit__(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

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


class GlassdoorScraper(WebScraper):
    def __init__(self, websocket_endpoint = "ws://localhost:3000/"):
        super().__init__(websocket_endpoint)
        self.url = "https://www.glassdoor.com/Job/united-states-data-engineer-jobs-SRCH_IL.0,13_IN1_KO14,27.htm?sortBy=date_desc&fromAge=1"
        self.closed_auth_modal = False

    async def scrape_job_listings(self):
        # Launch with context to use specific user agent settings / viewport settings
        # Browser is also heavier / more resource intensive
        page = await self.context.new_page()
        # Proceed once basic HTML loads
        await page.goto(self.url, wait_until="documentloaded", timeout=60000)
        # Wait for login auth modal pop up. Close if it appears.
        await self.close_auth_modal(page)
        # While there are more jobs to load, click load more button
        await self.load_more(page)
        # ul > li tells playwright to navigate to travel to the parent class and then list child classes nested under
        job_cards = page.locator("ul.JobsList_jobsList_lqjTr > li")
        
    async def close_auth_modal(self, page):
        """
        Closes the authentication modal if it appears.
        """
        # Set CSS path to modal button
        auth_modal_button = "button[data-test='auth-modal-close-button']"
        try:
            await page.wait_for_selector(auth_modal_button, state="visible", timeout=2500)
            print(f"Authentication modal detected. Closing...")
            # Click selector button
            await page.click(auth_modal_button)
            self.closed_auth_modal = True
        except:
            print(f"No authentication modal detected. Continuing with scraping...")
            pass

    async def load_more(self, page):
        """
        Clicks load more jobs if more job listings are available.
        """
        # Set CSS path to load more button
        # User locator insted of wait for selector to account for page changes after clicking load more button.
        load_more_button = page.locator("button[data-test='load-more']")
        while True:
            try:
                await load_more_button.wait_for(load_more_button, state="visible", timeout=6000)
                print(f"Load more button detected. Clicking...")
                # Click selector button
                await page.click(load_more_button)
                await page.wait_for_load_state("networkidle", timeout=5000)
                await self.close_auth_modal(page)
            except:
                print(f"No further load more buttons detected. Continuing with scraping...")
                break