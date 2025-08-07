import httpx
import os
import logging
from pydantic import BaseModel, ValidationError
from typing import List

from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

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

    async def get_all_airports(self) -> List[Airport]:  # type: ignore
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=10),
                retry=retry_if_exception_type(
                    (httpx.RequestError, httpx.HTTPStatusError)
                ),
                reraise=True,
            ):
                with attempt:
                    if attempt.retry_state.attempt_number > 1:
                        logger.warning(
                            f"Retrying airport fetch, attempt {attempt.retry_state.attempt_number}..."
                        )

                    async with httpx.AsyncClient() as client:
                        response = await client.get(f"{self.base_url}/airports/")
                        response.raise_for_status()

                        airports_data = response.json()
                        return [
                            Airport(**airport_data) for airport_data in airports_data
                        ]

        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            logger.error(f"An HTTP error occurred after all retries: {exc}")
            return []
        except (ValidationError, Exception) as exc:
            logger.error(
                f"An unexpected or data validation error occurred while fetching airports: {exc}"
            )
            return []
