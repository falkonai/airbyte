#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from unittest.mock import MagicMock

from airbyte_cdk.models import SyncMode
from pytest import fixture
from source_avoma.source import IncrementalAvomaStream
from source_avoma.thread_safe_counter import Counter


@fixture
def patch_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(IncrementalAvomaStream, "path", "v0/example_endpoint")
    mocker.patch.object(IncrementalAvomaStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalAvomaStream, "__abstractmethods__", set())


def test_cursor_field(patch_incremental_base_class):
    stream = IncrementalAvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    expected_cursor_field = "modified"
    assert stream.cursor_field == expected_cursor_field


def test_stream_slices(patch_incremental_base_class):
    stream = IncrementalAvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    inputs = {"sync_mode": SyncMode.incremental, "cursor_field": [], "stream_state": {}}
    expected_stream_slice = [None]
    assert stream.stream_slices(**inputs) == expected_stream_slice


def test_supports_incremental(patch_incremental_base_class, mocker):
    mocker.patch.object(IncrementalAvomaStream, "cursor_field", "dummy_field")
    stream = IncrementalAvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    assert stream.supports_incremental


def test_source_defined_cursor(patch_incremental_base_class):
    stream = IncrementalAvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    assert stream.source_defined_cursor


def test_stream_checkpoint_interval(patch_incremental_base_class):
    stream = IncrementalAvomaStream(authenticator=MagicMock(), api_counter=Counter(), config={"start_date": "2023-01-01T00:00:00Z"})
    expected_checkpoint_interval = None
    assert stream.state_checkpoint_interval == expected_checkpoint_interval
