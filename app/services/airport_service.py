import httpx
import os
import logging
import asyncio
from pydantic import BaseModel
from typing import List

logger = logging.getLogger(__name__)

REQUEST_RETRIES = 3
REQUEST_TIMEOUT = 60

class Airport(BaseModel):
    code: str
    name: str
    country: str

class AirportService:
    def init(self):
        self.base_url = os.getenv('MAIN_BACKEND_URL', 'http://localhost:8000')
        if not self.base_url:
            logger.critical("Backend URL is not set. AirportService cannot function.")
            raise ValueError("Backend URL cannot be empty")

    async def get_all_airports(self) -> List[Airport]:
        last_exception = None
        async with httpx.AsyncClient() as client:
            for attempt in range(REQUEST_RETRIES):
                try:
                    response = await client.get(
                        f"{self.base_url}/airports/",
                        timeout=REQUEST_TIMEOUT
                    )
                    response.raise_for_status()
                    airports_data = response.json()
                    logger.info("Successfully fetched airports.")
                    return [Airport(**airport_data) for airport_data in airports_data]
                except httpx.HTTPError as exc:
                    last_exception = exc
                    logger.warning(
                        f"Attempt {attempt + 1}/{REQUEST_RETRIES} to fetch airports failed: {exc}"
                    )
                    if attempt < REQUEST_RETRIES - 1:
                        await asyncio.sleep(2 * (attempt + 1))
                except Exception as exc:
                    last_exception = exc
                    logger.warning(
                        f"Attempt {attempt + 1}/{REQUEST_RETRIES} encountered an unexpected error: {exc}"
                    )
                    if attempt < REQUEST_RETRIES - 1:
                        await asyncio.sleep(2 * (attempt + 1))

        logger.error(f"FATAL: Failed to fetch airports after {REQUEST_RETRIES} attempts: {last_exception}")
        return []
