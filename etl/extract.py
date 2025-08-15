# etl/fetch.py
import requests
import time
from requests.exceptions import RequestException

class AnimalExtractor:

    def __init__(self, base_url):
        self.base_url = base_url

    def get_animal_detail(self, animal_id):
        """
        Fetch details for a single animal by ID, with retries.
        """
        url = f"{self.base_url}/{animal_id}"
        max_attempts = 5

        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                wait_time = 2 ** attempt
                print(f"Error fetching animal {animal_id}: {e}. Retrying in {wait_time}s (attempt {attempt}/{max_attempts})...")
                time.sleep(wait_time)

        raise Exception(f"Failed to fetch animal {animal_id} after {max_attempts} attempts.")

    def get_all_animals(self):
        """
        Fetch all animals from the paginated API, with retries per page.
        """
        all_animals = []
        page = 1
        max_page_retries = 5
        max_pages = None 

        while max_pages is None or page <= max_pages:
            retries = 0
            while retries < max_page_retries:
                try:
                    response = requests.get(self.base_url, params={"page": page}, timeout=20)
                    response.raise_for_status()
                    data = response.json()

                    # Set max_pages from first successful request
                    if max_pages is None:
                        max_pages = data.get("total_pages", page)
                        print(f"Total pages detected: {max_pages}")

                    items = data.get("items", [])
                    if not items:
                        # No more animals to fetch
                        return all_animals

                    # Fetch details for each animal on this page
                    for item in items:
                        animal_detail = self.get_animal_detail(item["id"])
                        all_animals.append(animal_detail)

                    # Move to next page
                    page += 1
                    break  # Exit retry loop if successful

                except RequestException as e:
                    retries += 1
                    wait_time = 2 ** retries
                    print(f"Error fetching page {page}: {e}. Retry {retries}/{max_page_retries} in {wait_time}s...")
                    time.sleep(wait_time)
            else:
                # Max retries exceeded for this page, skip it
                print(f"Failed to fetch page {page} after {max_page_retries} retries. Skipping page.")
                page += 1

        return all_animals
