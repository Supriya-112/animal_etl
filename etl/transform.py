# etl/transform.py
from dateutil import parser
from datetime import timezone

def transform_animal(animal):
    # convert : friends string -> list
    friends_str = animal.get("friends", "")
    animal["friends"] = [f.strip() for f in friends_str.split(",") if f.strip()]

    # convert : born_at -> ISO8601 UTC
    born_at = animal.get("born_at")
    if born_at and isinstance(born_at, str):  # only parse if it's a non-empty string
        try:
            dt = parser.parse(born_at)
            animal["born_at"] = dt.astimezone(timezone.utc).isoformat()
        except Exception as e:
            print(f"Warning: Could not parse born_at for animal {animal['id']}: {born_at}, error: {e}")
            animal["born_at"] = None
    else:
        animal["born_at"] = None

    return animal
