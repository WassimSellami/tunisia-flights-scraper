# app/services/nouvelair_scraper_service.py
import logging
import os
import time
from datetime import datetime
from itertools import product
from typing import List, Dict, Any
import requests
from playwright.sync_api import sync_playwright
from .shared_services import BackendApiClient

NOUVELAIR_AVAILABILITY_API = "https://webapi.nouvelair.com/api/reservation/availability"
NOUVELAIR_URL = "https://www.nouvelair.com/"
CURRENCY_ID = 2
AIRLINE_CODE = "BJ"

logger = logging.getLogger(__name__)

class NouvelairScraper:
    def __init__(self, api_client: BackendApiClient):
        self.api_client = api_client
        self.api_key = None

    def _capture_api_key(self):
        logger.info("Launching headless browser to capture API key...")
        captured_key = None
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            key_holder = []
            def handle_request(request):
                if "webapi.nouvelair.com/api" in request.url and "x-api-key" in request.headers and not key_holder:
                    key_holder.append(request.headers["x-api-key"])
            page.on("request", handle_request)
            try:
                page.goto(NOUVELAIR_URL, wait_until="domcontentloaded", timeout=45000)
                page.wait_for_function(lambda: len(key_holder) > 0, timeout=30000)
                captured_key = key_holder[0]
            except Exception as e:
                logger.error(f"Error during Playwright API key capture: {e}")
            finally:
                browser.close()
        if captured_key:
            self.api_key = captured_key
            logger.info("API Key successfully secured.")
        else:
            logger.error("Failed to capture API key.")

    def _get_nouvelair_flight_availability(self, dep_code: str, dest_code: str) -> List[Dict[str, Any]]:
        headers = {"User-Agent": "Mozilla/5.0", "Origin": NOUVELAIR_URL, "X-API-Key": self.api_key}
        params = {"departure_code": dep_code, "destination_code": dest_code, "trip_type": 1, "currency_id": CURRENCY_ID}
        try:
            res = requests.get(NOUVELAIR_AVAILABILITY_API, params=params, headers=headers, timeout=20)
            res.raise_for_status()
            return res.json().get("data", [])
        except requests.RequestException as e:
            logger.error(f"Error fetching availability for {dep_code}->{dest_code}: {e}")
            return []

    def run(self):
        logger.info("--- Starting Nouvelair scraper run ---")
        self._capture_api_key()
        if not self.api_key:
            logger.critical("Scraper run aborted: Could not obtain API key.")
            return

        airports = self.api_client.get_airports()
        if not airports:
            logger.critical("Scraper run aborted: Could not fetch airport list from backend.")
            return

        tunisian_airports = [a['code'] for a in airports if a.get("country") == "TN"]
        german_airports = [a['code'] for a in airports if a.get("country") == "DE"]
        routes = list(product(tunisian_airports, german_airports)) + list(product(german_airports, tunisian_airports))
        
        all_scraped_flights: List[Dict[str, Any]] = []
        for dep_code, arr_code in routes:
            for flight in self._get_nouvelair_flight_availability(dep_code, arr_code):
                try:
                    price = float(flight["price"])
                    if price <= 0: continue
                    all_scraped_flights.append({
                        "departureDate": datetime.strptime(flight["date"], "%Y-%m-%d").isoformat(),
                        "price": price, "priceEur": price,
                        "departureAirportCode": dep_code, "arrivalAirportCode": arr_code,
                        "airlineCode": AIRLINE_CODE,
                    })
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Skipping malformed flight record: {flight}. Error: {e}")
            time.sleep(1)

        try:
            self.api_client.report_scraped_data(all_scraped_flights)
        except Exception as e:
            logger.critical(f"A fatal error occurred while reporting Nouvelair data. Run aborted. Error: {e}")
            raise

        logger.info("--- Nouvelair scraper run finished successfully ---")