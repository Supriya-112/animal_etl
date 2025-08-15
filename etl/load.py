import requests
import time
from requests.exceptions import RequestException

from .utils import RetryHandler, Logger

class AnimalLoader:

    def __init__(self, url, batch_size):
        self.url = url
        self.batch_size = batch_size
        self.retry_handler = RetryHandler()
        self.logger = Logger.get_logger()

    def post_animals_batch(self, batch):
        self.logger.info(f"Posting batch of {len(batch)} animals...")
        self.retry_handler.request_with_retry("POST", self.url, json=batch, timeout=30)
        self.logger.info(f"Posted batch successfully.")

    def post_all_animals(self, animals, batch_size=100):
        for i in range(0, len(animals), batch_size):
            batch = animals[i:i+batch_size]
            self.post_animals_batch(batch)
