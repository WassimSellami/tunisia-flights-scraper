import httpx
import os
import logging
from pydantic import BaseModel
from typing import List

logger = logging.getLogger(__name__)


class Airport(BaseModel):
    code: str
    name: str
    country: str


class AirportService:
    def __init__(self):
        self.base_url = os.getenv("MAIN_BACKEND_URL", "http://localhost:8000")
        if not self.base_url:
            logger.critical("Backend URL is not set. AirportService cannot function.")
            raise ValueError("Backend URL cannot be empty")

    async def get_all_airports(self) -> List[Airport]:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{self.base_url}/airports/")
                response.raise_for_status()

                airports_data = response.json()
                return [Airport(**airport_data) for airport_data in airports_data]

            except httpx.RequestError as exc:
                logger.error(f"An HTTP error occurred while requesting airports: {exc}")
                return []
            except Exception as exc:
                logger.error(
                    f"An unexpected error occurred while fetching airports: {exc}"
                )
                return []
