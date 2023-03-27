#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .api import Outplay
from .stream import Accounts, Prospects, SequenceReports, Sequences, Users
from .thread_safe_counter import Counter


# Source
class SourceOutplay(AbstractSource):
    def __init__(self):
        self._api_counter = Counter()

    @staticmethod
    def _get_outplay_object(config: Mapping[str, Any], api_counter: Counter) -> Outplay:
        outplay = Outplay(**config, api_counter=api_counter)
        outplay.login()
        return outplay

    def check_connection(self, _, config) -> Tuple[bool, any]:
        try:
            outplay = self._get_outplay_object(config, api_counter=self._api_counter)
            outplay.account
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        args = {"config": config, "api_counter": self._api_counter}

        return [
            Accounts(**args),
            Prospects(**args),
            SequenceReports(**args),
            Sequences(**args),
            Users(**args),
        ]
