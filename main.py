from etl.config import Config
from etl.animal_etl_manager import AnimalETLManager
from etl.extract import AnimalExtractor
from etl.transform import AnimalTransformer
from etl.load import AnimalLoader

if __name__ == "__main__":
    cfg = Config()

    runner = AnimalETLManager(
        AnimalExtractor(cfg),
        AnimalTransformer(),
        AnimalLoader(cfg)
    )
    runner.run()

