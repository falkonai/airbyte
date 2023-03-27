#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_outplay.source import SourceOutplay


def test_check_connection(mocker, config):
    source = SourceOutplay()
    logger_mock = MagicMock()
    assert source.check_connection(logger_mock, config) == (True, None)
