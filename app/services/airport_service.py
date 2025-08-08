import logging
import time
from typing import List, Dict, Any
import requests

logger = logging.getLogger(__name__)

REQUEST_RETRIES = 3
REQUEST_TIMEOUT = 15


class BackendApiClient:
    def __init__(self, base_url: str):
        if not base_url:
            raise ValueError("Backend URL cannot be empty")
        self.base_url = base_url
        self.session = requests.Session()

    def get_airports(self) -> List[Dict[str, Any]]:
        """Fetch airport list from backend with retries similar to shared_services retry logic."""
        url = f"{self.base_url}/airports/"
        last_exception = None

        for attempt in range(REQUEST_RETRIES):
            try:
                logger.info(f"Fetching airports (attempt {attempt + 1}/{REQUEST_RETRIES})...")
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
                    # exponential-ish backoff like shared_services
                    time.sleep(2 * (attempt + 1))

        # If we exhausted all attempts
        logger.error(
            f"FATAL: Failed to fetch airports after {REQUEST_RETRIES} attempts. "
            f"Last error was: {last_exception}"
        )
        return []
