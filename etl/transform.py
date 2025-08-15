from dateutil import parser
from datetime import timezone

from .utils import Logger

class AnimalTransformer:

    def __init__(self):
        self.logger = Logger.get_logger()

    def transform(self, animal: dict) -> dict:
        self._transform_friends(animal)
        self._transform_born_at(animal)
        return animal
    
    def _transform_friends(self, animal):
        # convert : friends string -> list
        friends_str = animal.get("friends", "")
        animal["friends"] = [f.strip() for f in friends_str.split(",") if f.strip()]

    def _transform_born_at(self, animal):
        # convert : born_at -> ISO8601 UTC
        born_at = animal.get("born_at")
        if born_at and isinstance(born_at, str):  # only parse if it's a non-empty string
            try:
                dt = parser.parse(born_at)
                animal["born_at"] = dt.astimezone(timezone.utc).isoformat()
            except Exception as e:
                self.logger.info(f"Warning: Could not parse born_at for animal {animal['id']}: {born_at}, error: {e}")
                animal["born_at"] = None
        else:
            animal["born_at"] = None
