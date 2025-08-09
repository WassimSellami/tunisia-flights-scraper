import logging
import os
import time
from datetime import datetime, date
from itertools import product
from typing import List, Dict, Any, Tuple
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from .backend_api_client import BackendApiClient

BASE_URL_DE = "https://flights.tunisair.com/en-de/prices/per-day"
BASE_URL_TN = "https://flights.tunisair.com/en-tn/prices/per-day"
EXCHANGE_RATE_API_URL = "https://v6.exchangerate-api.com/v6/{api_key}/latest/TND"
AIRLINE_CODE = "TU"
MONTHS_TO_SEARCH = 4
DEFAULT_TRIP_TYPE = "O"
DEFAULT_TRIP_DURATION = "0"
REQUEST_RETRIES = 3

VALID_ROUTES_DE_TO_TN: List[Tuple[str, str]] = [
    ("MUC", "TUN"),
    ("MUC", "MIR"),
    ("MUC", "DJE"),
    ("FRA", "TUN"),
    ("FRA", "DJE"),
    ("DUS", "TUN"),
]
VALID_ROUTES_TN_TO_DE: List[Tuple[str, str]] = [
    ("TUN", "MUC"),
    ("TUN", "FRA"),
    ("TUN", "DUS"),
    ("MIR", "MUC"),
    ("DJE", "MUC"),
    ("DJE", "FRA"),
]

logger = logging.getLogger(__name__)


class TunisairScraper:
    def __init__(self, api_client: BackendApiClient, exchange_rate_api_key: str):
        self.api_client = api_client
        self.session = requests.Session()
        self.api_key_provided = (
            exchange_rate_api_key and exchange_rate_api_key != "YOUR_API_KEY"
        )
        self.exchange_rate_api_key = exchange_rate_api_key
        self.fallback_eur_rate = 0.29

    def _get_exchange_rate(self) -> float:
        if not self.api_key_provided:
            logger.warning(
                f"API Key not found. Using fallback rate: 1 TND = {self.fallback_eur_rate:.4f} EUR"
            )
            return self.fallback_eur_rate
        url = EXCHANGE_RATE_API_URL.format(api_key=self.exchange_rate_api_key)
        for attempt in range(REQUEST_RETRIES):
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                if data.get("result") == "success":
                    rate = data["conversion_rates"]["EUR"]
                    logger.info(
                        f"Successfully fetched exchange rate: 1 TND = {rate:.4f} EUR"
                    )
                    return rate
            except requests.RequestException as e:
                logger.warning(
                    f"Attempt {attempt + 1}/{REQUEST_RETRIES} to fetch exchange rate failed: {e}"
                )
                if attempt < REQUEST_RETRIES - 1:
                    time.sleep(1)
        logger.error(
            f"Failed to fetch exchange rate after {REQUEST_RETRIES} attempts. Using fallback."
        )
        return self.fallback_eur_rate

    def _extract_prices(
        self, html: str, is_eur_native: bool, conversion_rate: float
    ) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        found_flights = []
        for td in soup.find_all("td", class_="available"):
            date_str, price_div = td.get("data-departure"), td.find(
                "div", class_="val_price_offre"
            )
            if not (
                date_str
                and price_div
                and (price_text := price_div.get_text(strip=True))
                and price_text != "-"
            ):
                continue
            try:
                departure_date = datetime.strptime(date_str, "%Y-%m-%d")
                if is_eur_native and "EUR" in price_text:
                    price_str = (
                        price_text.replace(" ", "").replace(",", ".").replace("EUR", "")
                    )
                    price_val = round(float(price_str), 2)
                    flight_data = {"price": price_val, "priceEur": price_val}
                elif not is_eur_native and "TND" in price_text:
                    price_str = (
                        price_text.replace(" ", "").replace(",", ".").replace("TND", "")
                    )
                    price_tnd = round(float(price_str), 3)
                    flight_data = {
                        "price": price_tnd,
                        "priceEur": round(price_tnd * conversion_rate, 2),
                    }
                else:
                    continue
                flight_data["departureDate"] = departure_date.isoformat()
                found_flights.append(flight_data)
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Could not parse record '{date_str}' | '{price_text}'. Error: {e}"
                )
        return found_flights

    def _scrape_route(
        self,
        dep_code: str,
        arr_code: str,
        is_eur_native: bool,
        conversion_rate: float = 1.0,
    ) -> List[Dict[str, Any]]:
        base_url = BASE_URL_DE if is_eur_native else BASE_URL_TN
        route_flights = []
        today = date.today()
        search_dates = [today.strftime("%Y-%m-%d")] + [
            (today + relativedelta(months=i)).strftime("%Y-%m-01")
            for i in range(1, MONTHS_TO_SEARCH)
        ]
        for search_date in search_dates:
            params = {
                "date": search_date,
                "from": dep_code,
                "to": arr_code,
                "tripDuration": DEFAULT_TRIP_DURATION,
                "tripType": DEFAULT_TRIP_TYPE,
            }
            html_view = None
            for attempt in range(REQUEST_RETRIES):
                try:
                    response = self.session.get(base_url, params=params, timeout=20)
                    response.raise_for_status()
                    html_view = response.json().get("view", "")
                    break
                except requests.RequestException as e:
                    logger.warning(
                        f"Attempt {attempt + 1}/{REQUEST_RETRIES} failed for {dep_code}->{arr_code} on {search_date}: {e}"
                    )
                    if attempt < REQUEST_RETRIES - 1:
                        time.sleep(1)
            if html_view:
                for flight in self._extract_prices(
                    html_view, is_eur_native, conversion_rate
                ):
                    flight.update(
                        {
                            "departureAirportCode": dep_code,
                            "arrivalAirportCode": arr_code,
                            "airlineCode": AIRLINE_CODE,
                        }
                    )
                    route_flights.append(flight)
            else:
                logger.error(
                    f"Failed to fetch data for {dep_code}->{arr_code} on {search_date} after retries."
                )
            time.sleep(0.5)
        return route_flights

    def run(self):
        logger.info("--- Starting Tunisair scraper run ---")
        use_predefined = os.getenv("USE_PREDEFINED_ROUTES", "true").lower() in (
            "true",
            "1",
            "yes",
        )
        routes_de_tn = VALID_ROUTES_DE_TO_TN if use_predefined else []
        routes_tn_de = VALID_ROUTES_TN_TO_DE if use_predefined else []

        all_scraped_flights: List[Dict[str, Any]] = []
        logger.info("--- Scraping flights from Germany to Tunisia (EUR native) ---")
        for dep, arr in routes_de_tn:
            all_scraped_flights.extend(self._scrape_route(dep, arr, is_eur_native=True))

        logger.info("--- Scraping flights from Tunisia to Germany (TND native) ---")
        conversion_rate = self._get_exchange_rate()
        for dep, arr in routes_tn_de:
            all_scraped_flights.extend(
                self._scrape_route(
                    dep, arr, is_eur_native=False, conversion_rate=conversion_rate
                )
            )

        try:
            self.api_client.report_scraped_data(all_scraped_flights)
        except Exception as e:
            logger.critical(
                f"A fatal error occurred while reporting Tunisair data. Run aborted. Error: {e}"
            )
            raise

        logger.info("--- Tunisair scraper run finished successfully ---")
