#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib import parse
from urllib.parse import urljoin

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
from airbyte_cdk.sources.streams.http.auth.token import TokenAuthenticator

from .thread_safe_counter import Counter

_URL_BASE = "https://api.avoma.com/v1/"


# Basic full refresh stream
class AvomaStream(HttpStream, ABC):

    has_shown_total_count = False
    url_base = _URL_BASE
    primary_key = "uuid"
    page_size = 200

    def __init__(
        self,
        authenticator: HttpAuthenticator,
        api_counter: Counter,
        config: Dict,
        **kwargs,
    ):
        self.api_counter = api_counter
        self.start_date = config["start_date"]
        self.config = config
        super().__init__(authenticator=authenticator, **kwargs)

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        ret = super()._send_request(request=request, request_kwargs=request_kwargs)
        self.api_counter.increment()
        return ret

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Returns the token for the next page as per https://api.avoma.io/api/v2/docs#pagination.
        It uses cursor-based pagination, by sending the 'page[size]' and 'page[after]' parameters.
        """
        max_api_requests = self.config["max_api_requests"] or 1000
        value = self.api_counter.value()
        if value >= max_api_requests:
            return None

        try:
            next_page_url = response.json().get("next")
            params = parse.parse_qs(parse.urlparse(next_page_url).query)
            if not params or "page" not in params:
                return {}
            return {"page": params["page"][0]}
        except Exception as e:
            raise KeyError(f"error parsing next_page token: {e}")

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"page_size": self.page_size}
        if next_page_token and "page" in next_page_token:
            params["page"] = next_page_token["page"]

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        body = response.json()

        count = body.get("count")
        if count and not self.has_shown_total_count:
            print(f'Total count of "{self.path()}": {count}')
            self.has_shown_total_count = True

        results = body.get("results")
        if not results:
            return
        for element in results:
            yield element


# Basic incremental stream
class IncrementalAvomaStream(AvomaStream, IncrementalMixin):
    _cursor_value: Optional[Any] = None

    @property
    def cursor_field(self) -> str:
        return "modified"

    @property
    def state(self) -> Mapping[str, Any]:
        state = {self.cursor_field: self._cursor_value}

        return state

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

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

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(
            sync_mode=sync_mode,
            cursor_field=cursor_field,
            stream_slice=stream_slice,
            stream_state=stream_state,
        )
        for record in records:
            self._cursor_value = (
                max(record.get(self.cursor_field, self.start_date), self._cursor_value)
                if self._cursor_value is not None
                else record.get(self.cursor_field, self.start_date)
            )
            yield record

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )
        if self.cursor_field in stream_state:
            params[f"{self.cursor_field}__gte"] = stream_state[self.cursor_field]
        return params


class Meetings(IncrementalAvomaStream):
    """
    Meetings stream. Yields data from the GET /meetings endpoint.
    See https://dev694.avoma.com/#tag/Meetings
    """

    def path(self, **kwargs) -> str:
        return "meetings"


# Dependent stream
class DependentAvomaStream(AvomaStream, ABC):
    """
    Makes requests to a get endpoint which returns single objects based on the uuids from a dependent stream where multiple ids are retrieved.

    Note: This is a workaround until Avoma can support retrieving transcripts on a list/batch which would be a more performant way.
    """

    def __init__(
        self,
        **kwargs,
    ):
        self.dependent_api_response_json = None
        self.remaining_ids = None
        super().__init__(**kwargs)

    def _fill_out_remaining_ids(self) -> None:
        if self.dependent_api_response_json is None:
            request = self._session.prepare_request(
                requests.Request(
                    method="GET",
                    url=urljoin(_URL_BASE, self.dependent_object_name),
                    headers=self.authenticator.get_auth_header(),
                    params={"page_size": self.page_size},
                )
            )
            response = self._send(request, {})
            self.dependent_api_response_json = response.json()
            ids = map(
                lambda record: record[self.dependent_foreign_key],
                self.dependent_api_response_json["results"],
            )
            self.remaining_ids = list(filter(lambda id: id is not None, ids))

        while len(self.remaining_ids) < 1 and self.dependent_api_response_json.get("next") is not None:
            url = self.dependent_api_response_json.get("next")

            request = self._session.prepare_request(
                requests.Request(
                    method="GET",
                    url=url,
                    headers=self.authenticator.get_auth_header(),
                    params={"page_size": self.page_size},
                )
            )
            response = self._send(request, {})
            self.dependent_api_response_json = response.json()
            ids = map(
                lambda record: record[self.dependent_foreign_key],
                self.dependent_api_response_json["results"],
            )
            self.remaining_ids = list(filter(lambda id: id is not None, ids))

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Returns the token for the next page based on endpoints from avoma.
        It uses cursor-based pagination, by sending a 'page' parameter for the next page.
        """
        max_api_requests = self.config["max_api_requests"] or 1000
        value = self.api_counter.value()
        if value >= max_api_requests:
            return None

        try:
            self._fill_out_remaining_ids()
            if len(self.remaining_ids) < 1:
                return {}

            next_id = self.remaining_ids[0]
            self.remaining_ids = self.remaining_ids[1:]

            return {"id": next_id}
        except Exception as e:
            raise KeyError(f"error parsing next_page token: {e}")

    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        if next_page_token is None:
            self._fill_out_remaining_ids()
            assert len(self.remaining_ids) > 0
            next_id = self.remaining_ids[0]
            self.remaining_ids = self.remaining_ids[1:]
        else:
            next_id = next_page_token["id"]

        return urljoin(_URL_BASE, f"{self.object_name}/{next_id}/", allow_fragments=True)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        if not data:
            return

        yield data

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}


class Transcriptions(DependentAvomaStream):
    """
    Transcriptions stream. Yields data from the GET /transcriptions/{uuid} endpoint.
    See https://dev694.avoma.com/#tag/Transcriptions/paths/~1v1~1transcriptions~1%7Buuid%7D/get
    """

    primary_key = "uuid"
    dependent_object_name = "meetings"
    dependent_foreign_key = "transcription_uuid"
    object_name = "transcriptions"


# Source
class SourceAvoma(AbstractSource):
    def __init__(self):
        self._api_counter = Counter()

    def _create_authenticator(self, config) -> TokenAuthenticator:
        return TokenAuthenticator(token=config["bearer_token"])

    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            headers = self._create_authenticator(config).get_auth_header()
            response = requests.get(urljoin(_URL_BASE, "users/"), headers=headers)
            response.raise_for_status()
            return True, None
        except Exception as e:
            logger.error(f"Failed to check connection. Error: {e}")
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = self._create_authenticator(config)
        args = {
            "authenticator": auth,
            "config": config,
            "api_counter": self._api_counter,
        }
        return [
            Meetings(**args),
            Transcriptions(**args),
        ]
