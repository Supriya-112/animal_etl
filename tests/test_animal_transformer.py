import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from etl.transform import AnimalTransformer

@pytest.fixture
def transformer():
    return AnimalTransformer()

def test_transform_full_data(transformer):
    animal = {
        "id": 1,
        "friends": "Tom, Jerry , Spike",
        "born_at": "2020-05-01T15:30:00"
    }
    result = transformer.transform(animal)

    assert result["friends"] == ["Tom", "Jerry", "Spike"]
    dt = datetime.fromisoformat(result["born_at"])
    assert dt.tzinfo == timezone.utc

def test_transform_empty_friends(transformer):
    animal = {
        "id": 2,
        "friends": "",
        "born_at": "2022-01-01"
    }
    result = transformer.transform(animal)
    assert result["friends"] == []
    assert isinstance(result["born_at"], str)

def test_transform_missing_friends_key(transformer):
    animal = {
        "id": 3,
        "born_at": "2021-06-15T10:00:00Z"
    }
    result = transformer.transform(animal)
    assert result["friends"] == []

def test_transform_friends_with_extra_spaces(transformer):
    animal = {
        "id": 4,
        "friends": "   Tom  ,   , Jerry , ",
        "born_at": "2020-05-01"
    }
    result = transformer.transform(animal)
    assert result["friends"] == ["Tom", "Jerry"]

def test_transform_born_at_none(transformer):
    animal = {
        "id": 5,
        "friends": "Max, Bella",
        "born_at": None
    }
    result = transformer.transform(animal)
    assert result["born_at"] is None

def test_transform_born_at_not_a_string(transformer):
    animal = {
        "id": 6,
        "friends": "Max, Bella",
        "born_at": 12345 
    }
    result = transformer.transform(animal)
    assert result["born_at"] is None

@patch("etl.transform.Logger.get_logger")
def test_transform_invalid_born_at_logs_warning(mock_logger):
    mock_log_instance = MagicMock()
    mock_logger.return_value = mock_log_instance

    transformer = AnimalTransformer()
    animal = {
        "id": 7,
        "friends": "Tom",
        "born_at": "invalid-date"
    }
    result = transformer.transform(animal)

    assert result["born_at"] is None
    mock_log_instance.info.assert_called_once()
    assert "Could not parse born_at" in mock_log_instance.info.call_args[0][0]

def test_transform_with_missing_born_at_key(transformer):
    animal = {
        "id": 8,
        "friends": "Tom"
    }
    result = transformer.transform(animal)
    assert result["born_at"] is None
