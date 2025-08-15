import requests
import time
from requests.exceptions import RequestException

from .utils import RetryHandler, Logger
from .config import Config

class AnimalExtractor:

    def __init__(self, cfg: Config):
        self.base_url = cfg.get_animals_url()
        self.timeout = cfg.get_timeout()
        self.retry_handler = RetryHandler(cfg)
        self.logger = Logger.get_logger()

    def get_animal_detail(self, animal_id):
        """
        Fetch details for a single animal by ID, with retries.
        """
        url = f"{self.base_url}/{animal_id}"
        self.logger.info(f"Fetching animal {animal_id}...")
        resp = self.retry_handler.request_with_retry("GET", url, timeout=self.timeout)
        return resp.json()

    def get_all_animals(self):
        """
        Fetch all animals from the paginated API, with retries per page.
        """
        all_animals = []
        page = 1
        max_pages = None
        self.logger.info("Starting fetch of all animals...")

        while max_pages is None or page <= max_pages:
            try:
                resp = self.retry_handler.request_with_retry("GET", self.base_url, params={"page": page}, timeout=self.timeout)
                data = resp.json()
                if max_pages is None:
                    max_pages = data.get("total_pages", page)
                    self.logger.info(f"Total pages detected: {max_pages}")

                items = data.get("items", [])
                if not items:
                    break

                for item in items:
                    animal_detail = self.get_animal_detail(item["id"])
                    all_animals.append(animal_detail)

                page += 1
            except Exception as e:
                self.logger.warning(f"Failed to fetch page {page}: {e}. Skipping page.")
                page += 1

        self.logger.info(f"Fetched total {len(all_animals)} animals.")
        return all_animals
