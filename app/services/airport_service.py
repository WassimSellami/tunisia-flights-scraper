import logging
import time
from typing import List, Dict, Any
import requests

logger = logging.getLogger(__name__)


class BackendApiClient:
    def __init__(self, base_url: str):
        if not base_url:
            raise ValueError("Backend URL cannot be empty")
        self.base_url = base_url

    def get_airports(self, retries: int = 3, delay: int = 5) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/airports/"
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logger.warning(
                    f"Attempt {attempt + 1} of {retries} to fetch airports failed: {e}"
                )
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    logger.error(
                        f"FATAL: All {retries} attempts to fetch airports failed. The original error was: {e}"
                    )
        return []

    def report_scraped_data(self, data: List[Dict[str, Any]]):
        url = f"{self.base_url}/flights/report"
        try:
            response = requests.post(url, json=data, timeout=30)
            response.raise_for_status()
            logger.info(
                f"Successfully reported {len(data)} flight records to the backend."
            )
        except requests.RequestException as e:
            logger.error(f"Failed to report scraped data to backend: {e}")
            raise
