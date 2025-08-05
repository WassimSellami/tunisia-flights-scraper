import time
import logging
import requests
from itertools import product
from datetime import datetime
from playwright.sync_api import sync_playwright
from typing import List, Dict, Any

NOUVELAIR_AVAILABILITY_API = "https://webapi.nouvelair.com/api/reservation/availability"
NOUVELAIR_URL = "https://www.nouvelair.com/"
CURRENCY_ID = 2
AIRLINE_CODE = "BJ"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BackendApiClient:
    """A client to safely interact with your main flight backend API."""
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()

    def get_airports(self) -> List[Dict[str, Any]]:
        """Fetches all airports from the backend."""
        try:
            response = self.session.get(f"{self.base_url}/airports/")
            response.raise_for_status()
            logger.info("Successfully fetched airports from backend.")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"FATAL: Failed to fetch airports from backend: {e}")
            return []

    def report_scraped_data(self, scraped_flights: List[Dict[str, Any]]) -> bool:
        """Reports a batch of scraped flight data to the backend."""
        payload = {"flights": scraped_flights}
        logger.info(f"Reporting {len(scraped_flights)} found flights to the backend...")
        try:
            response = self.session.post(
                f"{self.base_url}/flights/report-scraped-data",
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            logger.info("Successfully reported scraped data. Backend accepted the report.")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to report scraped data to backend: {e}")
            if e.response is not None:
                logger.error(f"Backend responded with status {e.response.status_code}: {e.response.text}")
            return False


class NouvelairScraper:
    """Scrapes Nouvelair flights and reports them to the main backend."""
    def __init__(self, api_client: BackendApiClient):
        self.api_client = api_client
        self.api_key = None

    def _capture_api_key(self):
        """Launches a headless browser to capture the necessary X-API-Key."""
        logger.info("Launching headless browser to capture API key...")
        captured_key = None
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            key_found = False

            def handle_request(request):
                nonlocal captured_key, key_found
                if not captured_key and "webapi.nouvelair.com/api" in request.url and "x-api-key" in request.headers:
                    captured_key = request.headers["x-api-key"]
                    logger.info(f"API Key captured: {captured_key[:5]}...")
                    key_found = True
            
            page.on("request", handle_request)
            try:
                page.goto(NOUVELAIR_URL, wait_until="domcontentloaded", timeout=45000)
                page.wait_for_condition(lambda: key_found, timeout=30000)
            except Exception as e:
                logger.error(f"Error during Playwright API key capture: {e}")
            finally:
                browser.close()
        
        if captured_key:
            self.api_key = captured_key
            logger.info("API Key successfully secured.")
        else:
            logger.error("Failed to capture API key within the time limit.")

    def _get_nouvelair_flight_availability(self, departure_code: str, destination_code: str) -> List[Dict[str, Any]]:
        """Fetches monthly price data for a specific route from Nouvelair's API."""
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": NOUVELAIR_URL,
            "Origin": NOUVELAIR_URL,
            "Accept": "application/json",
            "X-API-Key": self.api_key,
        }
        params = {
            "departure_code": departure_code.upper(),
            "destination_code": destination_code.upper(),
            "trip_type": 1,
            "currency_id": CURRENCY_ID,
        }
        try:
            res = requests.get(NOUVELAIR_AVAILABILITY_API, params=params, headers=headers)
            res.raise_for_status()
            return res.json().get("data", [])
        except requests.RequestException as e:
            logger.error(f"Error fetching availability for {departure_code}->{destination_code}: {e}")
            return []

    def run(self):
        """Executes the full scraping and reporting process."""
        logger.info("Starting Nouvelair scraper run...")
        self._capture_api_key()
        if not self.api_key:
            logger.error("Scraper run aborted: Could not obtain API key.")
            return

        airports = self.api_client.get_airports()
        if not airports:
            logger.error("Scraper run aborted: Could not fetch airport list from backend.")
            return
            
        tunisian_airports = [a['code'] for a in airports if a.get("country") == "TN"]
        german_airports = [a['code'] for a in airports if a.get("country") == "DE"]

        routes = list(product(tunisian_airports, german_airports)) + \
                 list(product(german_airports, tunisian_airports))

        all_scraped_flights: List[Dict[str, Any]] = []
        for dep_code, arr_code in routes:
            logger.info(f"Scraping route: {dep_code} -> {arr_code}")
            flights_on_route = self._get_nouvelair_flight_availability(dep_code, arr_code)
            time.sleep(1)

            for flight_data in flights_on_route:
                try:
                    price = float(flight_data["price"])
                    if price <= 0: continue
                    
                    departure_date = datetime.strptime(flight_data["date"], "%Y-%m-%d")

                    scraped_flight = {
                        "departureDate": departure_date.isoformat(),
                        "price": price,
                        "priceEur": price,
                        "departureAirportCode": dep_code,
                        "arrivalAirportCode": arr_code,
                        "airlineCode": AIRLINE_CODE,
                    }
                    all_scraped_flights.append(scraped_flight)
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Skipping malformed flight record: {flight_data}. Error: {e}")

        if all_scraped_flights:
            self.api_client.report_scraped_data(all_scraped_flights)
        else:
            logger.info("No flights found in this scraping run.")
            
        logger.info("Scraper run finished.")
