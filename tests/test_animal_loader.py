import pytest
from unittest.mock import MagicMock, patch
from etl.load import AnimalLoader


@pytest.fixture
def mock_config():
    cfg = MagicMock()
    cfg.get_home_url.return_value = "http://fake-url.com"
    cfg.get_batch_size.return_value = 2
    cfg.get_timeout.return_value = 5
    return cfg


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_retry_handler():
    handler = MagicMock()
    handler.request_with_retry = MagicMock()
    return handler


@pytest.fixture
def loader(mock_config, mock_logger, mock_retry_handler):
    with patch("etl.load.Logger.get_logger", return_value=mock_logger), \
         patch("etl.load.RetryHandler", return_value=mock_retry_handler):
        return AnimalLoader(mock_config)


def test_post_animals_batch_calls_retry_handler(loader, mock_retry_handler):
    batch = [{"name": "Lion"}, {"name": "Tiger"}]
    loader.post_animals_batch(batch)

    loader.logger.info.assert_any_call("Posting batch of 2 animals...")
    mock_retry_handler.request_with_retry.assert_called_once_with(
        "POST",
        loader.url,
        json=batch,
        timeout=loader.timeout
    )
    loader.logger.info.assert_any_call("Posted batch successfully.")


def test_post_all_animals_splits_batches_correctly(loader, mock_retry_handler):
    animals = [{"name": f"Animal{i}"} for i in range(5)] 
    loader.batch_size = 2

    loader.post_all_animals(animals)

    # Expected batches: [0:2], [2:4], [4:5] => 3 batches
    assert mock_retry_handler.request_with_retry.call_count == 3
    calls = [call.kwargs["json"] for call in mock_retry_handler.request_with_retry.call_args_list]
    assert calls == [
        [{"name": "Animal0"}, {"name": "Animal1"}],
        [{"name": "Animal2"}, {"name": "Animal3"}],
        [{"name": "Animal4"}],
    ]


def test_post_all_animals_handles_empty_list(loader, mock_retry_handler):
    loader.post_all_animals([])
    mock_retry_handler.request_with_retry.assert_not_called()
    loader.logger.info.assert_not_called()


def test_post_animals_batch_propagates_exceptions(loader, mock_retry_handler):
    mock_retry_handler.request_with_retry.side_effect = Exception("Network error")
    with pytest.raises(Exception) as excinfo:
        loader.post_animals_batch([{"name": "Elephant"}])
    assert "Network error" in str(excinfo.value)


def test_post_all_animals_with_batch_size_of_one(loader, mock_retry_handler):
    loader.batch_size = 1
    animals = [{"name": "A"}, {"name": "B"}, {"name": "C"}]

    loader.post_all_animals(animals)

    assert mock_retry_handler.request_with_retry.call_count == 3
    calls = [call.kwargs["json"] for call in mock_retry_handler.request_with_retry.call_args_list]
    assert calls == [
        [{"name": "A"}],
        [{"name": "B"}],
        [{"name": "C"}],
    ]

def test_loader_initializes_from_config(mock_config, mock_logger, mock_retry_handler):
    with patch("etl.load.Logger.get_logger", return_value=mock_logger), \
         patch("etl.load.RetryHandler", return_value=mock_retry_handler):
        loader = AnimalLoader(mock_config)

    assert loader.url == "http://fake-url.com"
    assert loader.batch_size == 2
    assert loader.timeout == 5
    assert loader.retry_handler == mock_retry_handler
    assert loader.logger == mock_logger


def test_post_animals_batch_empty_list(loader, mock_retry_handler):
    loader.post_animals_batch([])
    loader.logger.info.assert_any_call("Posting batch of 0 animals...")
    mock_retry_handler.request_with_retry.assert_called_once()


def test_post_all_animals_divisible_batches(loader, mock_retry_handler):
    animals = [{"name": "A"}, {"name": "B"}, {"name": "C"}, {"name": "D"}]
    loader.batch_size = 2

    loader.post_all_animals(animals)

    assert mock_retry_handler.request_with_retry.call_count == 2
    calls = [call.kwargs["json"] for call in mock_retry_handler.request_with_retry.call_args_list]
    assert calls == [
        [{"name": "A"}, {"name": "B"}],
        [{"name": "C"}, {"name": "D"}],
    ]


def test_post_all_animals_stops_on_exception(loader, mock_retry_handler):
    animals = [{"name": "A"}, {"name": "B"}]
    loader.batch_size = 1
    mock_retry_handler.request_with_retry.side_effect = [None, Exception("Boom")]

    with pytest.raises(Exception, match="Boom"):
        loader.post_all_animals(animals)

    assert mock_retry_handler.request_with_retry.call_count == 2  # attempted first two batches


def test_post_animals_batch_respects_timeout(loader, mock_retry_handler):
    loader.timeout = 99
    batch = [{"name": "X"}]

    loader.post_animals_batch(batch)

    mock_retry_handler.request_with_retry.assert_called_once_with(
        "POST",
        loader.url,
        json=batch,
        timeout=99
    )
