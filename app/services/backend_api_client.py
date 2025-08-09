import logging
import time
from typing import List, Dict, Any
import requests

logger = logging.getLogger(__name__)

POST_CHUNK_SIZE = 150
REQUEST_RETRIES = 3
REQUEST_TIMEOUT = 60


class BackendApiClient:
    def __init__(self, base_url: str):
        if not base_url:
            raise ValueError("Backend base_url cannot be empty.")
        self.base_url = base_url
        self.session = requests.Session()

    def get_airports(self) -> List[Dict[str, Any]]:
        """Fetch airport list from backend with retries similar to shared_services retry logic."""
        url = f"{self.base_url}/airports/"
        last_exception = None

        for attempt in range(REQUEST_RETRIES):
            try:
                logger.info(
                    f"Fetching airports (attempt {attempt + 1}/{REQUEST_RETRIES})..."
                )
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                logger.info("Successfully fetched airports.")
                return response.json()
            except requests.RequestException as e:
                last_exception = e
                logger.warning(
                    f"Attempt {attempt + 1}/{REQUEST_RETRIES} to fetch airports failed: {e}"
                )
                if attempt < REQUEST_RETRIES - 1:
                    time.sleep(2 * (attempt + 1))

        logger.error(
            f"FATAL: Failed to fetch airports after {REQUEST_RETRIES} attempts. "
            f"Last error was: {last_exception}"
        )
        return []

    def report_scraped_data(self, scraped_flights: List[Dict[str, Any]]):
        if not scraped_flights:
            logger.info("No scraped flights to report.")
            return

        logger.info(
            f"Preparing to report {len(scraped_flights)} total flights in chunks..."
        )
        for i in range(0, len(scraped_flights), POST_CHUNK_SIZE):
            chunk = scraped_flights[i : i + POST_CHUNK_SIZE]
            payload = {"flights": chunk}
            chunk_number = i // POST_CHUNK_SIZE + 1
            logger.info(f"Reporting chunk {chunk_number} with {len(chunk)} flights...")

            last_exception = None
            for attempt in range(REQUEST_RETRIES):
                try:
                    response = self.session.post(
                        f"{self.base_url}/flights/report-scraped-data",
                        json=payload,
                        timeout=REQUEST_TIMEOUT,
                    )
                    response.raise_for_status()
                    last_exception = None
                    break
                except requests.RequestException as e:
                    last_exception = e
                    logger.warning(
                        f"Attempt {attempt + 1}/{REQUEST_RETRIES} for chunk {chunk_number} failed: {e}"
                    )
                    if attempt < REQUEST_RETRIES - 1:
                        time.sleep(2 * (attempt + 1))

            if last_exception:
                logger.error(
                    f"Failed to report chunk {chunk_number} after {REQUEST_RETRIES} attempts."
                )
                raise last_exception

            if i + POST_CHUNK_SIZE < len(scraped_flights):
                time.sleep(1)

        logger.info("All chunks reported successfully.")
