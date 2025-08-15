class AnimalETLManager:

    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        print("Fetching all animals...")
        animals = self.extractor.get_all_animals()
        print(f"Fetched {len(animals)} animals")

        print("Transforming animals...")
        transformed = [self.transformer.transform(a) for a in animals]

        print("Posting animals in batches...")
        self.loader.post_all_animals(transformed)

        print("ETL completed successfully!")
