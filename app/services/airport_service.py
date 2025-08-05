import httpx
from pydantic import BaseModel
from typing import List

# Define a Pydantic model to validate the structure of the airport data you receive.
# This is based on the AirportOut schema you provided.
class Airport(BaseModel):
    code: str
    name: str
    country: str

class AirportService:
    def __init__(self, base_url: str = 'https://tunisia-flights-backend.onrender.com'):
        """
        Initializes the service with the base URL of the flights backend.
        """
        self.base_url = base_url

    async def get_all_airports(self) -> List[Airport]:
        """
        Fetches all airport data from the backend API.
        """
        async with httpx.AsyncClient() as client:
            try:
                # Make a GET request to the /airports/ endpoint
                response = await client.get(f"{self.base_url}/airports/")
                
                # Raise an exception for HTTP errors (e.g., 404, 500)
                response.raise_for_status()
                
                # Parse the JSON response into a list of Airport objects
                airports_data = response.json()
                return [Airport(**airport_data) for airport_data in airports_data]

            except httpx.RequestError as exc:
                print(f"An error occurred while requesting airports: {exc}")
                return []
            except Exception as exc:
                print(f"An unexpected error occurred: {exc}")
                return []