import requests
import time
from requests.exceptions import RequestException

from .utils import RetryHandler, Logger
from .config import Config

class AnimalLoader:

    def __init__(self, cfg: Config):
        self.url = cfg.get_home_url()
        self.batch_size = cfg.get_batch_size()
        self.timeout = cfg.get_timeout()
        self.retry_handler = RetryHandler(cfg)
        self.logger = Logger.get_logger()

    def post_animals_batch(self, batch):
        self.logger.info(f"Posting batch of {len(batch)} animals...")
        self.retry_handler.request_with_retry("POST", self.url, json=batch, timeout=self.timeout)
        self.logger.info(f"Posted batch successfully.")

    def post_all_animals(self, animals):
        for i in range(0, len(animals), self.batch_size):
            batch = animals[i:i+self.batch_size]
            self.post_animals_batch(batch)
