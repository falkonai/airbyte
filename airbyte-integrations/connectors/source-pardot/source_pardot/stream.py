#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

from .thread_safe_counter import Counter


# Basic full refresh stream
class PardotStream(HttpStream, ABC):
    url_base = "https://pi.pardot.com/api/"
    api_version = "5"
    time_filter_template = "%Y-%m-%dT%H:%M:%S+00:00"
    primary_key = "id"
    is_integer_state = False
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)
    limit = 1000

    def __init__(self, config: Dict, api_counter: Counter, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self._api_counter = api_counter

    def _is_page_token_available(self, next_page_token: Optional[Mapping[str, Any]]):
        return next_page_token is not None and not next_page_token.get("reset_page_token", False)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        results = response.json()
        next_page_token = results.get("nextPageToken")
        pardot_warning_header = response.headers.get("Pardot-Warning")
        max_api_requests = self.config["max_api_requests"] or 5000
        value = self._api_counter.value()
        if value >= max_api_requests:
            return None
        if next_page_token and len(next_page_token) > 0:
            return {"nextPageToken": next_page_token}
        elif (
            pardot_warning_header is not None
            and isinstance(pardot_warning_header, str)
            and pardot_warning_header.find("Record count for nextPageToken sequence has been exceeded.") != -1
        ):
            # There's a pardot warning header when attempting to read more than 100,000 records which will stop using the page token.
            # Return a reset setting to know that we should make the next request based on state instead of the page token.
            return {"reset_page_token": True}
        else:
            # Returning None will discontinue pagination.
            return None

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        headers = {"Pardot-Business-Unit-Id": self.config["pardot_business_unit_id"]}
        return headers

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        schema = self.get_json_schema()
        fields = ",".join(schema["properties"].keys())
        params = {"fields": fields}
        if self._is_page_token_available(next_page_token):
            params.update(**next_page_token)
        else:
            start_date = self.config.get("start_date", None)
            if start_date and self.filter_param in {"createdAtAfter", "updatedAtAfter", "sentAtAfter"}:
                params.update({self.filter_param: pendulum.parse(start_date, strict=False).strftime(self.time_filter_template)})

            params.update({"limit": self.limit})
        return params

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        ret = super()._send_request(request=request, request_kwargs=request_kwargs)
        self._api_counter.increment()
        return ret

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json()
        values = results.get("values", [])
        for val in values:
            yield val

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"v{self.api_version}/objects/{self.object_name}"

    def filter_records_newer_than_state(
        self,
        stream_state: Mapping[str, Any] = None,
        records_slice: Mapping[str, Any] = None,
    ) -> Iterable:
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
    cursor_field = "id"
    filter_param = "idGreaterThan"
    is_integer_state = True


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
class PardotIncrementalReplicationStream(PardotStream, IncrementalMixin):
    order_by_field = "id"
    cursor_field = "updatedAt"
    filter_param = "updatedAtAfter"
    additional_filters: Optional[Mapping[str, Any]] = None
    state_checkpoint_interval = 1000
    _cursor_value: Optional[Any] = None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        if not self._is_page_token_available(next_page_token):
            params.update({"orderBy": self.order_by_field})
            if self.additional_filters is not None:
                params.update(self.additional_filters)
            cursor_field_value = stream_state.get(self.cursor_field, None) if stream_state is not None else None
            if cursor_field_value is not None:
                params.update({self.filter_param: cursor_field_value})

            # Helps migrate from "id" based state that switched to a date based filter.
            id_field_value = stream_state.get("id", None) if stream_state is not None else None
            if id_field_value is not None and self.cursor_field != "id":
                if self.filter_param in params:
                    del params[self.filter_param]
                params.update({"idGreaterThan": id_field_value})

        return params

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: str(self._cursor_value)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        for record in records:
            blank_val = 0 if self.is_integer_state else ""
            self._cursor_value = (
                max(record.get(self.cursor_field, blank_val), self._cursor_value)
                if self._cursor_value is not None
                else record.get(self.cursor_field, blank_val)
            )
            yield record


class VisitorActivities(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/visitor-activity-v5.html
    Note: Not including details field since it causes salesforce's api to timeout with a 504.
    """

    use_cache = True
    object_name = "visitor-activities"


class VisitorPageViews(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/visitor-page-view-v5.html
    """

    use_cache = True
    object_name = "visitor-page-views"
    cursor_field = "createdAt"
    filter_param = "createdAtAfter"


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
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/visitor-v5.html
    """

    use_cache = True
    object_name = "visitors"
    additional_filters = {"isIdentified": False}


class Campaigns(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/campaign-v5.html
    """

    object_name = "campaigns"


class ListMemberships(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/list-membership-v5.html
    """

    object_name = "list-memberships"


class Emails(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/email-v5.html
    """

    object_name = "emails"
    cursor_field = "id"
    filter_param = "idGreaterThan"
    is_integer_state = True


class Visits(PardotIncrementalReplicationStream):
    """
    API documentation: https://developer.salesforce.com/docs/marketing/pardot/guide/visit-v5.html
    """

    object_name = "visits"
