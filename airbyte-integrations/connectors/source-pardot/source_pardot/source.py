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


# Source
class SourcePardot(AbstractSource):
    @staticmethod
    def _get_pardot_object(config: Mapping[str, Any]) -> Pardot:
        pardot = Pardot(**config)
        pardot.login()
        return pardot

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            pardot = self._get_pardot_object(config)
            pardot.access_token
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        pardot = self._get_pardot_object(config)
        auth = TokenAuthenticator(pardot.access_token)
        args = {"authenticator": auth, "config": config}

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
