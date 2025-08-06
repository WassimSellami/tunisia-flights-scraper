import time
import logging
import requests
import json
import os
from datetime import datetime, date
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from itertools import product
from typing import List, Dict, Any, Tuple

BASE_URL_DE = "https://flights.tunisair.com/en-de/prices/per-day"
BASE_URL_TN = "https://flights.tunisair.com/en-tn/prices/per-day"
EXCHANGE_RATE_API_URL = "https://v6.exchangerate-api.com/v6/{api_key}/latest/TND"
AIRLINE_CODE = "TU"

MONTHS_TO_SEARCH = 4
DEFAULT_TRIP_TYPE = "O"
DEFAULT_TRIP_DURATION = "0"

VALID_ROUTES_DE_TO_TN: List[Tuple[str, str]] = [
    ('MUC', 'TUN'), ('MUC', 'MIR'), ('MUC', 'DJE'),
    ('FRA', 'TUN'), ('FRA', 'DJE'),
    ('DUS', 'TUN'),
]
VALID_ROUTES_TN_TO_DE: List[Tuple[str, str]] = [
    ('TUN', 'MUC'), ('TUN', 'FRA'), ('TUN', 'DUS'),
    ('MIR', 'MUC'),
    ('DJE', 'MUC'), ('DJE', 'FRA'),
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BackendApiClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()

    def get_airports(self) -> List[Dict[str, Any]]:
        try:
            response = self.session.get(f"{self.base_url}/airports/")
            response.raise_for_status()
            logger.info("Successfully fetched airports from backend.")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"FATAL: Failed to fetch airports from backend: {e}")
            return []

    def report_scraped_data(self, scraped_flights: List[Dict[str, Any]]) -> bool:
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

class TunisairScraper:
    def __init__(self, api_client: BackendApiClient, exchange_rate_api_key: str):
        self.api_client = api_client
        self.session = requests.Session()
        self.api_key_provided = exchange_rate_api_key and exchange_rate_api_key != "YOUR_API_KEY"
        self.exchange_rate_api_key = exchange_rate_api_key
        self.fallback_eur_rate = 0.29

    def _get_exchange_rate(self) -> float:
        if not self.api_key_provided:
            logger.warning(f"API Key not found. Using fallback exchange rate: 1 TND = {self.fallback_eur_rate:.4f} EUR")
            return self.fallback_eur_rate
        
        url = EXCHANGE_RATE_API_URL.format(api_key=self.exchange_rate_api_key)
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('result') == 'success':
                rate = data['conversion_rates']['EUR']
                logger.info(f"Successfully fetched exchange rate: 1 TND = {rate:.4f} EUR")
                return rate
        except requests.RequestException as e:
            logger.error(f"Error fetching exchange rate: {e}. Using fallback.")
        
        logger.warning(f"Using fallback exchange rate: 1 TND = {self.fallback_eur_rate:.4f} EUR")
        return self.fallback_eur_rate

    def _extract_prices(self, html: str, is_eur_native: bool, conversion_rate: float) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        found_flights = []
        
        for td in soup.find_all("td", class_="available"):
            date_str = td.get("data-departure")
            price_div = td.find("div", class_="val_price_offre")

            if not (date_str and price_div and (price_text := price_div.get_text(strip=True)) and price_text != "-"):
                continue

            try:
                departure_date = datetime.strptime(date_str, "%Y-%m-%d")
                
                if is_eur_native and "EUR" in price_text:
                    price_str = price_text.replace(" ", "").replace(",", ".").replace("EUR", "")
                    price_val = round(float(price_str), 2)
                    flight_data = {"price": price_val, "priceEur": price_val}
                elif not is_eur_native and "TND" in price_text:
                    price_str = price_text.replace(" ", "").replace(",", ".").replace("TND", "")
                    price_val = round(float(price_str), 3)
                    price_eur = round(price_val * conversion_rate, 2)
                    flight_data = {"price": price_val, "priceEur": price_eur}
                else:
                    continue
                
                flight_data["departureDate"] = departure_date.isoformat()
                found_flights.append(flight_data)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse record with date '{date_str}' and price '{price_text}'. Error: {e}")
        return found_flights

    def run(self):
        logger.info("Starting Tunisair scraper run...")
        
        routes_de_to_tn: List[Tuple[str, str]] = []
        routes_tn_to_de: List[Tuple[str, str]] = []

        use_predefined = os.getenv('USE_PREDEFINED_ROUTES', 'true').lower() in ('true', '1', 'yes')

        if use_predefined:
            logger.info("Using predefined routes for scraping.")
            routes_de_to_tn = VALID_ROUTES_DE_TO_TN
            routes_tn_to_de = VALID_ROUTES_TN_TO_DE
        else:
            logger.info("Dynamically generating all possible routes from backend airports.")
            airports = self.api_client.get_airports()
            if not airports:
                logger.error("Scraper run aborted: Could not fetch airport list from backend.")
                return
            
            tunisian_airports = [a['code'] for a in airports if a.get("country") == "TN"]
            german_airports = [a['code'] for a in airports if a.get("country") == "DE"]
            
            if not tunisian_airports or not german_airports:
                logger.error("Could not find airports for both Germany and Tunisia to generate dynamic routes.")
                return

            routes_de_to_tn = list(product(german_airports, tunisian_airports))
            routes_tn_to_de = list(product(tunisian_airports, german_airports))

        all_scraped_flights: List[Dict[str, Any]] = []

        logger.info("--- Scraping flights from Germany to Tunisia (EUR native) ---")
        for dep_code, arr_code in routes_de_to_tn:
            all_scraped_flights.extend(self._scrape_route(dep_code, arr_code, is_eur_native=True))

        logger.info("--- Scraping flights from Tunisia to Germany (TND native) ---")
        conversion_rate = self._get_exchange_rate()
        for dep_code, arr_code in routes_tn_to_de:
            all_scraped_flights.extend(self._scrape_route(dep_code, arr_code, is_eur_native=False, conversion_rate=conversion_rate))

        if all_scraped_flights:
            self.api_client.report_scraped_data(all_scraped_flights)
        else:
            logger.info("No Tunisair flights found in this scraping run.")
            
        logger.info("Tunisair scraper run finished.")

    def _scrape_route(self, dep_code: str, arr_code: str, is_eur_native: bool, conversion_rate: float = 1.0) -> List[Dict[str, Any]]:
        logger.info(f"Scraping route: {dep_code} -> {arr_code}")
        
        base_url = BASE_URL_DE if is_eur_native else BASE_URL_TN
        
        today = date.today()
        search_dates = [today.strftime("%Y-%m-%d")]
        for i in range(1, MONTHS_TO_SEARCH):
            search_dates.append((today + relativedelta(months=i)).strftime("%Y-%m-01"))

        route_flights = []
        for search_date in search_dates:
            params = {
                "date": search_date,
                "from": dep_code,
                "to": arr_code,
                "tripDuration": DEFAULT_TRIP_DURATION,
                "tripType": DEFAULT_TRIP_TYPE
            }
            try:
                response = self.session.get(base_url, params=params, timeout=20)
                response.raise_for_status()
                data = response.json()
                html_view = data.get('view', '')
                logger.info(f"HTML content length: {len(html_view)}")

                
                if html_view:
                    extracted_data = self._extract_prices(html_view, is_eur_native, conversion_rate)
                    for flight_data in extracted_data:
                        flight_data["departureAirportCode"] = dep_code
                        flight_data["arrivalAirportCode"] = arr_code
                        flight_data["airlineCode"] = AIRLINE_CODE
                        route_flights.append(flight_data)
                
                time.sleep(0.5)
            except requests.RequestException as e:
                logger.error(f"Request failed for {dep_code}->{arr_code} on date {search_date}: {e}")
                continue
        
        logger.info(f"Found {len(route_flights)} prices for route {dep_code} -> {arr_code}")
        return route_flights