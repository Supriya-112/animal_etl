import yaml
import os

class Config:
    def __init__(self, config_file="config.yaml"):
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file not found: {config_file}")
        with open(config_file, "r") as f:
            self.config = yaml.safe_load(f)

    def get_animals_url(self):
        return self.config.get("animals_url")

    def get_home_url(self):
        return self.config.get("home_url")

    def get_batch_size(self):
        return self.config.get("batch_size", 100)

    def get_max_attempts(self):
        return self.config.get("max_attempts", 5)

    def get_backoff_factor(self):
        return self.config.get("backoff_factor", 2)

    def get_timeout(self):
        return self.config.get("timeout_seconds", 30)
