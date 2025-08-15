import time
import requests
from requests.exceptions import RequestException
import logging
import os
from datetime import datetime

from .config import Config

class RetryHandler:

    def __init__(self, cfg: Config):
        self.max_attempts = cfg.get_max_attempts()
        self.backoff_factor = cfg.get_backoff_factor()

    def request_with_retry(self, method, url, **kwargs):
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = requests.request(method, url, **kwargs)
                if resp.status_code in (500, 502, 503, 504):
                    raise RequestException(f"Server error {resp.status_code}")
                resp.raise_for_status()
                return resp
            except RequestException as e:
                wait_time = self.backoff_factor ** attempt
                Logger.get_logger().warning(f"{method} request failed for {url}: {e}. Retrying in {wait_time}s (attempt {attempt}/{self.max_attempts})...")
                time.sleep(wait_time)
        raise Exception(f"Failed to {method} {url} after {self.max_attempts} attempts")


class Logger:
    _logger = None

    @staticmethod
    def get_logger():
        if Logger._logger is None:
            os.makedirs("logs", exist_ok=True)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            log_file = f"logs/etl_{timestamp}.log"

            Logger._logger = logging.getLogger("ETLLogger")
            Logger._logger.setLevel(logging.INFO)

            # File handler
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.INFO)

            # Console handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)

            # Formatter
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            Logger._logger.addHandler(fh)
            Logger._logger.addHandler(ch)
        return Logger._logger
