# etl/load.py
import requests
import time
from requests.exceptions import RequestException

HOME_URL = "http://localhost:3123/animals/v1/home"

def post_animals_batch(batch):
    for attempt in range(5):
        try:
            resp = requests.post(HOME_URL, json=batch, timeout=30)
            if resp.status_code in (500, 502, 503, 504):
                raise RequestException(f"Server error {resp.status_code}")
            resp.raise_for_status()

            print(f"Posted batch of {len(batch)} animals successfully")
            return
        except RequestException:
            wait = 2 ** attempt
            print(f"Retrying batch in {wait}s...")
            time.sleep(wait)
    raise Exception("Failed to post batch after multiple retries")

def post_all_animals(animals, batch_size=100):
    for i in range(0, len(animals), batch_size):
        batch = animals[i:i+batch_size]
        post_animals_batch(batch)
