#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from source_avoma.source import AvomaStream
from source_avoma.thread_safe_counter import Counter


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(AvomaStream, "path", "v0/example_endpoint")
    mocker.patch.object(AvomaStream, "primary_key", "test_primary_key")
    mocker.patch.object(AvomaStream, "__abstractmethods__", set())


def test_request_params(patch_base_class):
    stream = AvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = {"page_size": 200}
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class):
    stream = AvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    response = MagicMock()
    response.json.return_value = {"next": "http://api.avoma.com/api/v1/meetings?page_size=200&page=2"}
    inputs = {"response": response}
    expected_token = {"page": "2"}
    assert stream.next_page_token(**inputs) == expected_token


def test_parse_response(patch_base_class):
    stream = AvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    response = MagicMock()
    response.json.return_value = {"results": [{"uuid": "123", "is_rep": True}]}
    inputs = {"response": response}
    expected_parsed_object = {"uuid": "123", "is_rep": True}
    assert next(stream.parse_response(**inputs)) == expected_parsed_object


def test_request_headers(patch_base_class):
    stream = AvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_headers = {}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class):
    stream = AvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = AvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = AvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time
