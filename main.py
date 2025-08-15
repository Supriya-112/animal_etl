from etl.animal_etl_manager import AnimalETLManager
from etl.extract import AnimalExtractor
from etl.transform import AnimalTransformer
from etl.load import AnimalLoader

if __name__ == "__main__":
    runner = AnimalETLManager(
        AnimalExtractor(base_url = "http://localhost:3123/animals/v1/animals"),
        AnimalTransformer(),
        AnimalLoader(url = "http://localhost:3123/animals/v1/home", batch_size=100)
    )
    runner.run()

