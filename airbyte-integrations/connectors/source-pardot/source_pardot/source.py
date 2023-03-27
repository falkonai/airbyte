#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from .api import Pardot
from .stream import (
    Campaigns,
    Emails,
    ListEmails,
    ListMemberships,
    Lists,
    Opportunities,
    ProspectAccounts,
    Prospects,
    Users,
    VisitorActivities,
    VisitorPageViews,
    Visitors,
    Visits,
)
from .thread_safe_counter import Counter


# Source
class SourcePardot(AbstractSource):
    def __init__(self):
        self._api_counter = Counter()

    @staticmethod
    def _get_pardot_object(config: Mapping[str, Any], api_counter: Counter) -> Pardot:
        pardot = Pardot(**config, api_counter=api_counter)
        pardot.login()
        return pardot

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            pardot = self._get_pardot_object(config, api_counter=self._api_counter)
            pardot.access_token
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        pardot = self._get_pardot_object(config, api_counter=self._api_counter)
        auth = TokenAuthenticator(pardot.access_token)
        args = {"authenticator": auth, "config": config, "api_counter": self._api_counter}

        visitors = Visitors(**args)

        return [
            Emails(**args),
            Campaigns(**args),
            ListMemberships(**args),
            ListEmails(**args),
            Lists(**args),
            ProspectAccounts(**args),
            Prospects(**args),
            Users(**args),
            VisitorPageViews(**args),
            VisitorActivities(**args),
            visitors,
            Visits(**args),
            Opportunities(**args),
        ]
