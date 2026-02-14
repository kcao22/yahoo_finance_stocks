from playwright.async_api import async_playwright


class GlassdoorScraper:
    def __init__(self, websocket_endpoint: str = "ws://localhost:3000/"):
        self.websocket_endpoint = websocket_endpoint  # Websocket = two-way tunnel to containerized playwright browser
        self.browser = None
        self.context = None
        
    async def __aenter__(self):
        """
        Method for async with to understand how 
        """
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.connect(self.websocket_endpoint)
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        return self
    
    


# https://www.glassdoor.com/Job/united-states-data-engineer-jobs-SRCH_IL.0,13_IN1_KO14,27.htm?sortBy=date_desc&fromAge=1