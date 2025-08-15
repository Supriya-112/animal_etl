import pytest
from unittest.mock import MagicMock, patch
from requests.models import Response
import json

from etl.extract import AnimalExtractor

@pytest.fixture
def mock_config():
    cfg = MagicMock()
    cfg.get_animals_url.return_value = "http://fakeapi.com/animals"
    cfg.get_timeout.return_value = 5
    return cfg

@pytest.fixture
def mock_retry_handler():
    return MagicMock()

@pytest.fixture
def animal_extractor(mock_config, mock_retry_handler):
    with patch("etl.extract.RetryHandler", return_value=mock_retry_handler), \
         patch("etl.extract.Logger.get_logger") as mock_logger:
        mock_logger.return_value = MagicMock()
        extractor = AnimalExtractor(mock_config)
    return extractor

def make_response(json_data, status_code=200):
    """Helper to create a fake requests.Response object."""
    resp = Response()
    resp.status_code = status_code
    resp._content = json.dumps(json_data).encode("utf-8")
    return resp

def test_get_animal_detail_success(animal_extractor, mock_retry_handler):
    mock_retry_handler.request_with_retry.return_value = make_response({"id": 1, "name": "Lion"})
    result = animal_extractor.get_animal_detail(1)
    assert result["name"] == "Lion"
    mock_retry_handler.request_with_retry.assert_called_once_with(
        "GET", "http://fakeapi.com/animals/1", timeout=5
    )

def test_get_animal_detail_failure(animal_extractor, mock_retry_handler):
    mock_retry_handler.request_with_retry.side_effect = Exception("Network error")
    with pytest.raises(Exception) as excinfo:
        animal_extractor.get_animal_detail(1)
    assert "Network error" in str(excinfo.value)

def test_get_all_animals_single_page(animal_extractor, mock_retry_handler):
    page1_data = {"total_pages": 1, "items": [{"id": 1}, {"id": 2}]}
    mock_retry_handler.request_with_retry.side_effect = [
        make_response(page1_data),  # first call for page list
        make_response({"id": 1, "name": "Lion"}), 
        make_response({"id": 2, "name": "Tiger"}), 
    ]
    animals = animal_extractor.get_all_animals()
    assert len(animals) == 2
    assert animals[0]["name"] == "Lion"
    assert animals[1]["name"] == "Tiger"

def test_get_all_animals_multiple_pages(animal_extractor, mock_retry_handler):
    page1 = {"total_pages": 2, "items": [{"id": 1}]}
    page2 = {"items": [{"id": 2}]}
    mock_retry_handler.request_with_retry.side_effect = [
        make_response(page1),
        make_response({"id": 1, "name": "Lion"}),
        make_response(page2),
        make_response({"id": 2, "name": "Tiger"}),
    ]
    animals = animal_extractor.get_all_animals()
    assert len(animals) == 2
    assert {a["name"] for a in animals} == {"Lion", "Tiger"}

def test_get_all_animals_empty_items(animal_extractor, mock_retry_handler):
    mock_retry_handler.request_with_retry.return_value = make_response({"total_pages": 1, "items": []})
    animals = animal_extractor.get_all_animals()
    assert animals == []

def test_get_all_animals_missing_total_pages(animal_extractor, mock_retry_handler):
    data = {"items": [{"id": 1}]}
    mock_retry_handler.request_with_retry.side_effect = [
        make_response(data),
        make_response({"id": 1, "name": "Elephant"}),
    ]
    animals = animal_extractor.get_all_animals()
    assert len(animals) == 1
    assert animals[0]["name"] == "Elephant"

def test_get_all_animals_page_failure(animal_extractor, mock_retry_handler):
    page1 = {"total_pages": 2, "items": [{"id": 1}]}
    mock_retry_handler.request_with_retry.side_effect = [
        make_response(page1),                         # Page 1
        make_response({"id": 1, "name": "Lion"}),
        Exception("Page fetch error"),                # Page 2 fails
    ]
    animals = animal_extractor.get_all_animals()
    assert len(animals) == 1  # Page 2 skipped

def test_get_all_animals_detail_failure(animal_extractor, mock_retry_handler):
    page1 = {"total_pages": 1, "items": [{"id": 1}]}
    mock_retry_handler.request_with_retry.side_effect = [
        make_response(page1),               # page
        Exception("Detail fetch error"),    # fail detail
    ]
    animals = animal_extractor.get_all_animals()
    assert len(animals) == 0  # Detail fetch failed so no animals

def test_get_animal_detail_non_json_response(animal_extractor, mock_retry_handler):
    # return a response with invalid JSON
    bad_resp = Response()
    bad_resp.status_code = 200
    bad_resp._content = b"not-json"
    mock_retry_handler.request_with_retry.return_value = bad_resp

    with pytest.raises(Exception):
        animal_extractor.get_animal_detail(1)


def test_get_animal_detail_non_200_status(animal_extractor, mock_retry_handler):
    mock_retry_handler.request_with_retry.return_value = make_response({"err": "fail"}, status_code=500)
    result = animal_extractor.get_animal_detail("123")
    assert result == {"err": "fail"} 


def test_get_all_animals_missing_items_key(animal_extractor, mock_retry_handler):
    mock_retry_handler.request_with_retry.return_value = make_response({"total_pages": 1})
    animals = animal_extractor.get_all_animals()
    assert animals == []


def test_get_all_animals_retry_returns_none(animal_extractor, mock_retry_handler):
    mock_retry_handler.request_with_retry.return_value = None
    animals = animal_extractor.get_all_animals()
    assert animals == []
