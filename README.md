🐾 Animal ETL Manager

A simple ETL (Extract, Transform, Load) system for fetching animal data from an API, transforming it, and loading it to a target endpoint.

This project demonstrates Pythonic ETL patterns with logging, retries, batch posting, and robust error handling.

⚙️ Configuration

All configuration is stored in `config.yaml`. Example:

```yaml
animals_url: "https://api.example.com/animals"
home_url: "https://my-target-endpoint.com/animals"
batch_size: 100
max_attempts: 5
backoff_factor: 2
timeout_seconds: 30
```

You can override `config_file` when instantiating `Config`:

```python
from etl.config import Config
cfg = Config("my_config.yaml")
```
🐍 Dependencies

Install dependencies with `pip`:
```shell
pip install -r requirements.txt
```

Dependencies include:

* `requests` – HTTP requests

* `python-dateutil` – Parse dates

* `pyyaml` – Config files

* `pytest` – Testing

* `tenacity` – Retry (optional, if used in future)

🖥️ Virtual Environment
Mac/Linux
```shell
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Windows (CMD)
```shell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Windows (PowerShell)
```shell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

🐾 Logging

All logs are written to the `logs/` folder automatically.

* File: `logs/etl_YYYY-MM-DD_HH-MM-SS.log`
* Console output also displays log info

Example log entry:
```shell
2025-08-15 14:30:05 - INFO - Fetching animal 123...
2025-08-15 14:30:10 - WARNING - Failed to fetch page 2: Network error. Skipping page.
```

🚀 Running the ETL
```shell
python main.py
```

Console output:
```shell
2025-08-15 20:35:03,434 - INFO - Fetching animal 219...
2025-08-15 20:35:03,452 - INFO - Fetched total 220 animals.
Fetched 220 animals
Transforming animals...
Posting animals in batches...
2025-08-15 20:35:03,453 - INFO - Posting batch of 100 animals...
2025-08-15 20:35:03,468 - INFO - Posted batch successfully.
2025-08-15 20:35:03,468 - INFO - Posting batch of 100 animals...
2025-08-15 20:35:03,487 - INFO - Posted batch successfully.
2025-08-15 20:35:03,487 - INFO - Posting batch of 20 animals...
2025-08-15 20:35:03,503 - INFO - Posted batch successfully.
ETL completed successfully!
```

🧪 Running Tests

This project uses `pytest`. Tests cover Extractor, Transformer, Loader.

Run all tests:
```shell
pytest tests/ -v --maxfail=1 --disable-warnings
```

Coverage (optional):
```shell
pip install pytest-cov
pytest --cov=etl tests/
```

Notes:

* Tests mock API calls and retries.

* Make sure `logs/` exists or Logger will create it automatically.

* Edge cases are tested, including empty pages, failed requests, non-JSON responses, and batch posting.

📝 Class Overview

`AnimalExtractor`

* Fetches animals from a paginated API.

* Supports retries for each request.

* Logs each fetch attempt.

`AnimalTransformer`

* Converts "friends" from comma-separated string to list.

* Normalizes "born_at" to ISO8601 UTC.

* Logs transformation warnings.

`AnimalLoader`

* Posts animals in batches to a target endpoint.

* Supports configurable batch size and retries.

* Logs each batch post.

`AnimalETLManager`

* Orchestrates ETL flow: extract → transform → load.

* Provides simple .run() interface for the full ETL.

🥳 Tips & Fun Facts

* Adjust batch_size to balance performance and load on your target API.

* Logs are timestamped, so you can trace any historical run.

* The ETL is idempotent if API supports duplicates filtering.