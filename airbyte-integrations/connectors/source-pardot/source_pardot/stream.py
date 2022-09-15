#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer


# Basic full refresh stream
class PardotStream(HttpStream, ABC):
    url_base = "https://pi.pardot.com/api/"
    api_version = "5"
    time_filter_template = "%Y-%m-%dT%H:%M:%SZ"
    primary_key = "id"
    is_integer_state = False
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)
    limit = 200

    def __init__(self, config: Dict, **kwargs):
        super().__init__(**kwargs)
        self.config = config

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        results = response.json()
        next_page_token = results.get("nextPageToken")
        if next_page_token and len(next_page_token) > 0:
            return {"nextPageToken": next_page_token}

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        headers = {"Pardot-Business-Unit-Id": self.config["pardot_business_unit_id"]}
        return headers

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        schema = self.get_json_schema()
        fields = ",".join(schema["properties"].keys())
        params = {"fields": fields}
        if next_page_token is not None:
            params.update(**next_page_token)
        else:
            start_date = self.config.get("start_date", None)
            if start_date:
                params.update({"createdAfter": pendulum.parse(start_date, strict=False).strftime(self.time_filter_template)})

            params.update({"limit": self.limit})
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json()
        values = results.get("values", [])
        # The result may be a dict if one record is returned
        if values is not None:
            for val in values:
                yield val

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"v{self.api_version}/objects/{self.object_name}"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        blank_val = 0 if self.is_integer_state else ""
        return {
            self.cursor_field: max(latest_record.get(self.cursor_field, blank_val), current_stream_state.get(self.cursor_field, blank_val))
        }

    def filter_records_newer_than_state(self, stream_state: Mapping[str, Any] = None, records_slice: Mapping[str, Any] = None) -> Iterable:
        if stream_state and records_slice is not None:
            for record in records_slice:
                if record[self.cursor_field] >= stream_state.get(self.cursor_field):
                    yield record
        elif records_slice is not None:
            yield from records_slice


# PardotFullReplicationStreams (doesn't filter via the api since filters are not available via salesforce.)
class ProspectAccounts(PardotStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/prospect-account-v5.html
    """

    object_name = "prospect-accounts"
    cursor_field = "createdAt"
    filter_param = "createdAtAfter"


class Opportunities(PardotStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/opportunity-v5.html
    """

    object_name = "opportunities"
    cursor_field = "createdAt"
    filter_param = "createdAtAfter"


class Users(PardotStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/user-v5.html
    """

    object_name = "users"
    cursor_field = "createdAt"
    filter_param = "createdAtAfter"


# PardotIncrementalReplicationStream
class PardotIncrementalReplicationStream(PardotStream):
    order_by_field = "id"
    cursor_field = "updatedAt"
    filter_param = "updatedAtAfter"
    additional_filters: Optional[Mapping[str, Any]] = None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        if next_page_token is None:
            params.update({"orderBy": self.order_by_field})
            if self.additional_filters is not None:
                params.update(self.additional_filters)
            cursor_field_value = stream_state.get(self.cursor_field, None) if stream_state is not None else None
            if cursor_field_value is not None:
                params.update({self.filter_param: cursor_field_value})
        return params


class VisitorActivities(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/visitor-activity-v5.html
    Note: Not including details field since it causes salesforce's api to timeout with a 504.
    """

    use_cache = True
    object_name = "visitor-activities"
    cursor_field = "id"
    filter_param = "idGreaterThan"


class VisitorPageViews(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/visitor-page-view-v5.html
    """

    use_cache = True
    object_name = "visitor-page-views"
    cursor_field = "id"
    filter_param = "idGreaterThan"


class Lists(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/list-v5.html
    """

    object_name = "lists"


class ListEmails(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/list-email-v5.html
    """

    object_name = "list-emails"


class Prospects(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/prospect-v5.html
    """

    object_name = "prospects"


class Visitors(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/visitors-v4.html
    """

    use_cache = True
    object_name = "visitors"
    additional_filters = {"isIdentified": False}


class Campaigns(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/campaigns-v4.html
    """

    cursor_field = "id"
    filter_param = "idGreaterThan"
    object_name = "campaigns"
    is_integer_state = True


class ListMemberships(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/list-membership-v5.html
    """

    cursor_field = "id"
    filter_param = "idGreaterThan"
    object_name = "list-memberships"
    is_integer_state = True


class Emails(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/email-v5.html
    """

    object_name = "emails"
    cursor_field = "sentAt"
    filter_param = "sentAtAfter"


class Visits(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/visit-v5.html
    """

    object_name = "visits"
    filter_param = "idGreaterThan"
    cursor_field = "id"
    is_integer_state = True
