# main.py
from etl.extract import fetch_all_animals
from etl.transform import transform_animal
from etl.load import post_all_animals

def run_etl():
    print("Fetching all animals...")
    animals = fetch_all_animals()
    print(f"Fetched {len(animals)} animals")

    print("Transforming animals...")
    transformed = [transform_animal(a) for a in animals]

    print("Posting animals in batches...")
    post_all_animals(transformed)

    print("ETL completed successfully!")

if __name__ == "__main__":
    run_etl()
