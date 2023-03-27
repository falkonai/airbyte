#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

from .data_helper import retrieve_date_from_mapping
from .thread_safe_counter import Counter


# Basic full refresh stream
class OutplayStream(HttpStream, ABC):
    api_version = "1"
    primary_key = "id"
    time_filter_template = "%Y-%m-%dT%H:%M:%S+00:00"
    is_integer_state = False
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization | TransformConfig.CustomSchemaNormalization)

    def __init__(self, config: Dict, api_counter: Counter, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self._api_counter = api_counter

    @transformer.registerCustomTransform
    def handle_empty_strings(original_value: Any, field_schema: Dict[str, Any]) -> Optional[Any]:
        # Instead of returning null an empty string is returned via their api. Which doesn't work for integer types. So convert those to nulls.
        if (
            "type" in field_schema
            and "null" in field_schema["type"]
            and "integer" in field_schema["type"]
            and isinstance(original_value, str)
            and len(original_value) == 0
        ):
            return None
        return original_value

    @property
    def url_base(self) -> str:
        return f"https://{self.config.get('location') or 'us4'}-api.outplayhq.com/api/"

    def _is_page_token_available(self, next_page_token: Optional[Mapping[str, Any]]):
        return next_page_token is not None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        results = response.json()
        pagination = results.get("pagination", None) if isinstance(results, dict) else None
        has_more_records = pagination.get("hasmorerecords", False) if pagination is not None else False
        current_page = pagination.get("page", 1) if pagination is not None else 1
        max_api_requests = self.config["max_api_requests"] or 10000
        value = self._api_counter.value()
        if value >= max_api_requests:
            return None
        if has_more_records and current_page > 0:
            return {"pageindex": current_page + 1}
        else:
            # Returning None will discontinue pagination.
            return None

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        headers = {"X-CLIENT-SECRET": self.config["client_secret"], "Content-Type": "application/json"}
        return headers

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {"client_id": self.config["client_id"]}

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        params = {"pageindex": 1}
        if self._is_page_token_available(next_page_token):
            params.update(**next_page_token)
        else:
            start_date = self.config.get("start_date", None)
            if start_date and self.cursor_field in {"CreatedDate", "UpdatedDate"}:
                params.update(
                    {
                        "fieldvalue": pendulum.parse(start_date, strict=False).strftime(self.time_filter_template),
                        "fieldcondition": "gte",
                        "fieldname": self.cursor_field,
                    }
                )

        return params

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        ret = super()._send_request(request=request, request_kwargs=request_kwargs)
        self._api_counter.increment()
        return ret

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json()
        values = results if isinstance(results, list) else results.get("data", [])
        for val in values:
            yield val

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"v{self.api_version}/{self.object_name}"

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


# OutplayFullReplicationStreams (doesn't filter via the api since filters are not available via salesforce.)
class Users(OutplayStream):
    """
    API documentation: https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24
    """

    http_method = "GET"
    object_name = "team"
    cursor_field = "userid"
    primary_key = "userid"


class Accounts(OutplayStream):
    """
    API documentation: https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24
    """

    http_method = "POST"
    object_name = "prospectaccount/search"
    primary_key = "accountid"
    cursor_field = "accountid"
    cursor_field_condition = "gte"


class Sequences(OutplayStream):
    """
    API documentation: https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24
    """

    http_method = "POST"
    object_name = "sequence/search"
    primary_key = "sequenceid"
    cursor_field = "sequenceid"
    cursor_field_condition = "gte"


# OutplayIncrementalReplicationStream
class OutplayIncrementalReplicationStream(OutplayStream, IncrementalMixin):
    http_method = "POST"
    cursor_field_condition = "gte"
    additional_filters: Optional[Mapping[str, Any]] = None
    state_checkpoint_interval = 1000
    _cursor_value: Optional[int] = None

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_body_json(stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["orderBy"] = self.cursor_field

        cursor_field_value = stream_state.get(self.cursor_field, None) if stream_state is not None else None
        if self._cursor_value is not None or cursor_field_value is not None:
            params["fieldname"] = self.cursor_field
            params["fieldvalue"] = int(self._cursor_value) if self._cursor_value is not None else cursor_field_value
            params["fieldcondition"] = self.cursor_field_condition

        return params

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field] if self.cursor_field in value else None

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        for record in records:
            record_value = record.get(self.cursor_field, 0)
            self._cursor_value = max(record_value, self._cursor_value) if self._cursor_value is not None else record_value
            yield record

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        record_value = latest_record.get(self.cursor_field, 0)
        cursor_field_value = current_stream_state.get(self.cursor_field, 0)
        return {self.cursor_field: max(record_value, cursor_field_value)}

    def filter_records_newer_than_state(
        self,
        stream_state: Mapping[str, Any] = None,
        records_slice: Mapping[str, Any] = None,
    ) -> Iterable:
        if self._cursor_value is not None and records_slice is not None:
            for record in records_slice:
                if record[self.cursor_field] >= self._cursor_value:
                    yield record
        elif records_slice is not None:
            yield from records_slice


class Prospects(OutplayIncrementalReplicationStream):
    """
    API documentation: https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24
    """

    use_cache = True
    object_name = "prospect/search"
    primary_key = "prospectid"
    cursor_field = "prospectid"


# OutplayReportDateIncrementalReplicationStream
class OutplayReportDateIncrementalReplicationStream(OutplayStream, IncrementalMixin):
    http_method = "GET"
    cursor_field = "reportdate"
    date_filter_template = "%Y-%m-%d"
    additional_filters: Optional[Mapping[str, Any]] = None
    state_checkpoint_interval = 1000
    _cursor_value: Optional[str] = None
    _last_request_cursor_value: Optional[date] = None

    def _is_page_token_available(self, next_page_token: Optional[Mapping[str, Any]]):
        return next_page_token is not None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        results = response.json()
        report_date = results[0].get("report_date", None) if isinstance(results, list) and len(results) > 0 else None

        if report_date is None:
            report_date = self._last_request_cursor_value or pendulum.parse(self.config.get("start_date", None), strict=False).date()

        next_date = report_date + timedelta(days=1)
        max_api_requests = self.config["max_api_requests"] or 10000
        value = self._api_counter.value()
        if value >= max_api_requests:
            return None
        if next_date < pendulum.now().date():
            return {
                "from": next_date.strftime(self.date_filter_template),
                "to": next_date.strftime(self.date_filter_template),
            }
        else:
            # Returning None will discontinue pagination.
            return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice=stream_slice, next_page_token=next_page_token)

        if self._is_page_token_available(next_page_token):
            params.update(**next_page_token)

        cursor_field_date_value = retrieve_date_from_mapping(stream_state, self.cursor_field)
        start_date = (
            pendulum.parse(self.config.get("start_date"), strict=False).date()
            if "start_date" in self.config and self.config.get("start_date") is not None
            else None
        )
        report_date = self._last_request_cursor_value or cursor_field_date_value or start_date

        if "from" not in params:
            params["from"] = report_date.strftime(self.date_filter_template)
        if "to" not in params:
            params["to"] = report_date.strftime(self.date_filter_template)

        params["type"] = 1

        self._last_request_cursor_value = pendulum.parse(params["from"], strict=False).date()

        return params

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json()
        values = results if isinstance(results, list) else results.get("data", [])
        for val in values:
            yield val

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field] if self.cursor_field in value else None

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        for record in records:
            record_date_value = retrieve_date_from_mapping(record, self.cursor_field)
            cursor_value_date = pendulum.parse(self._cursor_value, strict=False).date() if self._cursor_value is not None else None
            max_date = max(record_date_value, cursor_value_date) if cursor_value_date is not None else record_date_value
            self._cursor_value = max_date.strftime(self.date_filter_template) if max_date is not None else None
            yield record

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        record_date_value = retrieve_date_from_mapping(latest_record, self.cursor_field)
        cursor_field_date_value = retrieve_date_from_mapping(current_stream_state, self.cursor_field)
        return {
            self.cursor_field: max(record_date_value, cursor_field_date_value)
            if record_date_value is not None and cursor_field_date_value is not None
            else record_date_value or cursor_field_date_value
        }

    def filter_records_newer_than_state(
        self,
        stream_state: Mapping[str, Any] = None,
        records_slice: Mapping[str, Any] = None,
    ) -> Iterable:
        if self._cursor_value is not None and records_slice is not None:
            for record in records_slice:
                if self._cursor_value is not None or record[self.cursor_field] >= pendulum.parse(self._cursor_value, strict=False).date():
                    yield record
        elif records_slice is not None:
            yield from records_slice


class SequenceReports(OutplayReportDateIncrementalReplicationStream):
    """
    API documentation: https://documenter.getpostman.com/view/16947449/TzsikPV1#0bb4bb2d-6b8b-45d2-932e-588131c69e24
    """

    use_cache = True
    object_name = "reports/sequencereport"
    primary_key = ["sequenceid", "reportdate"]
