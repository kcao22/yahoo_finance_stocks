from playwright.async_api import async_playwright
from playwright_stealth import Stealth

class WebScraper:
    def __init__(self, websocket_endpoint: str):
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
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
        locator = await page.locator(locator_class)
        if await locator.count() > 0:
            text = await locator.first.inner_text()
            print(f"{locator_desc}: {text}")
            return text
        else:
            print(f"No {locator_desc} detected.")
            return "N/A"


class GlassdoorScraper(WebScraper):
    def __init__(self, websocket_endpoint = "ws://playwright-browser:3000/"):
        super().__init__(websocket_endpoint)
        self.url = "https://www.glassdoor.com/Job/united-states-data-engineer-jobs-SRCH_IL.0,13_IN1_KO14,27.htm?sortBy=date_desc&fromAge=1"
        self.closed_auth_modal = False

    async def scrape_job_listings(self) -> list[dict]:
        # Launch with context to use specific user agent settings / viewport settings
        # Browser is also heavier / more resource intensive
        page = await self.context.new_page()
        # Proceed once basic HTML loads
        print(f"Navigating to {self.url}...")
        await page.goto(self.url, wait_until="domcontentloaded", timeout=60000)
        # Wait for login auth modal pop up. Close if it appears
        await self.close_auth_modal(page)
        # While there are more jobs to load, click load more button
        await self.load_more(page)
        # ul > li tells playwright to navigate to travel to the parent class and then list child classes nested under
        job_cards = page.locator("ul.JobsList_jobsList_lqjTr > li")
        print(f"Found {await job_cards.count()} jobs posted. Scraping job data...")
        all_job_details = []
        for i in range(await job_cards.count()):
            job_details = {}
            f"Scraping job {i + 1} of {await job_cards.count()}..."
            card = job_cards.nth(i)
            # Scroll if job card is below current snapshot of screen
            await card.scroll_into_view_if_needed()
            await card.click()
            await self.close_auth_modal(page)
            job_details["company_name"] = await self.locate_text(
                page=page,
                locator_class="div[class*='EmployerProfile_employerNameHeading']",
                locator_desc="Company Name"
            )
            job_details["company_rating"] = await self.locate_text(
                page=page,
                locator_class="div[class*='RatingSingleStarContainer'] span",
                locator_desc="Company Rating"
            )
            title_row = page.locator("div[class*='JobDetails_employerAndJobTitle']")
            job_title = await title_row.locator("h1").inner_text()
            job_details["job_title"] = job_title
            print(f"Job title: {job_title}")
            location_locator = page.locator("[data-test='location]")
            job_details["location"] = await location_locator.inner_text()
            print(f"Job location: {job_details['location']}")
            salary_locator = page.locator("[data-test='detailSalary]")
            job_details["salary"] = await salary_locator.inner_text()
            print(f"Salary: {job_details['salary']}")
            all_job_details.append(job_details)
        return all_job_details
        

    async def get_job_description_details(self, page, job_details: dict) -> dict:
        # Locate job details box
        # Partial match on container name
        job_details_container = page.locator("div[class*='JobDetails_jobDetailsContainer']")
        # Expand job description
        self.click_button(
            page,
            button_selector="button[data-test='show-more-cta']",
            selector_desc="show more job description button",
            state="visible",
            timeout=2500
        )
        # Locate job description box
        # Similar to job details, partial container name match
        description_locator = job_details_container.locator("div[class*='JobDetails_jobDescription']")
        # Await for job description to load
        await description_locator.wait_for(state="visible", timeout=1500)
        job_details["description"] = await description_locator.inner_text()
        print(f"Job description: {job_details['description'][:50]}...")
        return job_details




        
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