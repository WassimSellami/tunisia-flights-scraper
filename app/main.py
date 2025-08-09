from fastapi import FastAPI
from contextlib import asynccontextmanager
import os
import logging
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

from app.services.backend_api_client import BackendApiClient
from app.services.nouvelair_scraper_service import NouvelairScraper
from app.services.tunisair_scraper_service import TunisairScraper

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_dotenv()
    logger.info("Scraper service starting up...")

    backend_url = os.getenv("MAIN_BACKEND_URL")
    exchange_api_key = os.getenv("EXCHANGE_RATE_API_KEY", "")

    if not backend_url:
        logger.critical(
            "FATAL: MAIN_BACKEND_URL environment variable is not set. Scrapers cannot run."
        )
        yield
        return

    api_client = BackendApiClient(base_url=backend_url)
    nouvelair_scraper = NouvelairScraper(api_client=api_client)
    tunisair_scraper = TunisairScraper(
        api_client=api_client, exchange_rate_api_key=exchange_api_key
    )

    def run_nouvelair_job():
        logger.info("--- Triggering Nouvelair scraper cron job ---")
        try:
            nouvelair_scraper.run()
            logger.info("--- Nouvelair scraper cron job finished ---")
        except Exception as e:
            logger.error(f"--- Nouvelair scraper cron job FAILED. Error: {e} ---")

    def run_tunisair_job():
        logger.info("--- Triggering Tunisair scraper cron job ---")
        try:
            tunisair_scraper.run()
            logger.info("--- Tunisair scraper cron job finished ---")
        except Exception as e:
            logger.error(f"--- Tunisair scraper cron job FAILED. Error: {e} ---")

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(run_nouvelair_job, "cron", minute=16)
    scheduler.add_job(run_tunisair_job, "cron", minute=10)
    scheduler.start()

    yield

    logger.info("Shutting down scheduler...")
    scheduler.shutdown()
    logger.info("Scraper service has shut down.")


app = FastAPI(lifespan=lifespan)


@app.get("/ping")
async def ping():
    return {"message": "pong"}
