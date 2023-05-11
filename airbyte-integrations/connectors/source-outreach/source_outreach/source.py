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
from airbyte_cdk.sources.streams.http.auth.oauth import Oauth2Authenticator

from .thread_safe_counter import Counter

_TOKEN_REFRESH_ENDPOINT = "https://api.outreach.io/oauth/token"
_URL_BASE = "https://api.outreach.io/api/v2/"


# Basic full refresh stream
class OutreachStream(HttpStream, ABC):

    url_base = _URL_BASE
    primary_key = "id"
    page_size = 1000

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

    def _send_request(
        self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]
    ) -> requests.Response:
        ret = super()._send_request(request=request, request_kwargs=request_kwargs)
        self.api_counter.increment()
        return ret

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        Returns the token for the next page as per https://api.outreach.io/api/v2/docs#pagination.
        It uses cursor-based pagination, by sending the 'page[size]' and 'page[after]' parameters.
        """
        max_api_requests = self.config["max_api_requests"] or 5000
        value = self.api_counter.value()
        if value >= max_api_requests:
            return None
        try:
            next_page_url = response.json().get("links").get("next")
            params = parse.parse_qs(parse.urlparse(next_page_url).query)
            if not params or "page[after]" not in params:
                return {}
            return {"after": params["page[after]"][0]}
        except Exception as e:
            raise KeyError(f"error parsing next_page token: {e}")

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"page[size]": self.page_size, "count": "false"}
        if next_page_token and "after" in next_page_token:
            params["page[after]"] = next_page_token["after"]

        if (
            "refresh_token" in stream_state
            and self.authenticator.cycling_refresh_token is None
        ):
            self.authenticator.refresh_token = stream_state["refresh_token"]

        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        data = response.json().get("data")
        if not data:
            return
        for element in data:
            relationships: Dict[str, List[int]] = dict()
            for r_type, relations in element.get("relationships").items():
                relationships[f"{r_type}"] = []
                if relations.get(
                    "data"
                ):  # Manage None and pass empty data. Some relationships only have links we do not handle these.
                    data = relations.get("data", [])

                    if isinstance(
                        data, dict
                    ):  # Manage some relationships that only have one element and are set as dict.
                        # instead of having [{'type': 'sequenceState', 'id': 1}] we have {'type': 'sequenceState', 'id': 1}
                        data = [data]

                    relationships[f"{r_type}"] = [e.get("id") for e in data]

            yield {
                **element.get("attributes"),
                **{self.primary_key: element[self.primary_key], **relationships},
            }


# Basic incremental stream
class IncrementalOutreachStream(OutreachStream, IncrementalMixin):
    _cursor_value: Optional[Any] = None

    @property
    def cursor_field(self) -> str:
        return "updatedAt"

    @property
    def state(self) -> Mapping[str, Any]:
        state = {self.cursor_field: self._cursor_value}
        # Really just a place to handle cycling refresh tokens that has access to the state.
        if self.authenticator.cycling_refresh_token is not None:
            state["refresh_token"] = self.authenticator.cycling_refresh_token

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
        params["sort"] = self.cursor_field
        if self.cursor_field in stream_state:
            params["filter[updatedAt]"] = stream_state[self.cursor_field] + "..inf"
        return params


class Prospects(IncrementalOutreachStream):
    """
    Prospect stream. Yields data from the GET /prospects endpoint.
    See https://api.outreach.io/api/v2/docs#prospect
    """

    def path(self, **kwargs) -> str:
        return "prospects"


class Sequences(IncrementalOutreachStream):
    """
    Sequence stream. Yields data from the GET /sequences endpoint.
    See https://api.outreach.io/api/v2/docs#sequence
    """

    def path(self, **kwargs) -> str:
        return "sequences"


class SequenceStates(IncrementalOutreachStream):
    """
    Sequence stream. Yields data from the GET /sequences endpoint.
    See https://api.outreach.io/api/v2/docs#sequenceState
    """

    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "sequenceStates"


# CreatedAt incremental stream
class CreatedAtIncrementalOutreachStream(OutreachStream, ABC):
    _cursor_value: Optional[Any] = None

    @property
    def cursor_field(self) -> str:
        return "createdAt"

    @property
    def state(self) -> Mapping[str, Any]:
        state = {self.cursor_field: self._cursor_value}
        # Really just a place to handle cycling refresh tokens that has access to the state.
        if self.authenticator.cycling_refresh_token is not None:
            state["refresh_token"] = self.authenticator.cycling_refresh_token

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
        params["sort"] = self.cursor_field
        if self.cursor_field in stream_state:
            params["filter[createdAt]"] = stream_state[self.cursor_field] + "..inf"
        return params


class Events(CreatedAtIncrementalOutreachStream):
    """
    Event stream. Yields data from the GET /events endpoint.
    See https://api.outreach.io/api/v2/docs#events
    """

    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "events"


# Dependent stream
class DependentOutreachStream(OutreachStream, ABC):
    """
    Makes requests to a get endpoint which returns single objects based on the ids from a dependent stream where multiple ids are retrieved.

    Note: This is a workaround for a bug where the templates endpoint doesn't return all of the sequenceTemplates' templates. This will retrieve all
    templates that are from SequenceTemplates.
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
                    params={"page[size]": self.page_size, "count": "false"},
                )
            )
            response = self._send(request, {})
            self.dependent_api_response_json = response.json()
            ids = map(
                lambda record: record["relationships"][self.relationship_object_name][
                    "data"
                ]["id"],
                self.dependent_api_response_json["data"],
            )
            self.remaining_ids = list(filter(lambda id: id is not None, ids))

        while (
            len(self.remaining_ids) < 1
            and self.dependent_api_response_json.get("links").get("next") is not None
        ):
            url = self.dependent_api_response_json.get("links").get("next")

            request = self._session.prepare_request(
                requests.Request(
                    method="GET",
                    url=url,
                    headers=self.authenticator.get_auth_header(),
                    params={"page[size]": self.page_size, "count": "false"},
                )
            )
            response = self._send(request, {})
            self.dependent_api_response_json = response.json()
            ids = map(
                lambda record: record["relationships"][self.relationship_object_name][
                    "data"
                ]["id"],
                self.dependent_api_response_json["data"],
            )
            self.remaining_ids = list(filter(lambda id: id is not None, ids))

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        Returns the token for the next page as per https://api.outreach.io/api/v2/docs#pagination.
        It uses cursor-based pagination, by sending the 'page[size]' and 'page[after]' parameters.
        """
        max_api_requests = self.config["max_api_requests"] or 5000
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

        return f"{_URL_BASE}/{self.object_name}/{next_id}"

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        data = response.json().get("data")
        if not data:
            return

        relationships: Dict[str, List[int]] = dict()
        for r_type, relations in data.get("relationships").items():
            relationships[f"{r_type}"] = []
            if relations.get(
                "data"
            ):  # Manage None and pass empty data. Some relationships only have links we do not handle these.
                relation_data = relations.get("data", [])

                if isinstance(
                    relation_data, dict
                ):  # Manage some relationships that only have one element and are set as dict.
                    # instead of having [{'type': 'sequenceState', 'id': 1}] we have {'type': 'sequenceState', 'id': 1}
                    relation_data = [relation_data]

                relationships[f"{r_type}"] = [e.get("id") for e in relation_data]

        yield {
            **data.get("attributes"),
            **{self.primary_key: data[self.primary_key], **relationships},
        }

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}


class Templates(DependentOutreachStream):
    """
    Template stream. Yields data from the GET /templates/{id} endpoint.
    See https://api.outreach.io/api/v2/docs#templates
    """

    primary_key = "id"
    dependent_object_name = "sequenceTemplates"
    object_name = "templates"
    relationship_object_name = "template"


class OutreachAuthenticator(Oauth2Authenticator):
    def __init__(
        self,
        redirect_uri: str,
        token_refresh_endpoint: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        api_counter: Counter,
    ):
        super().__init__(
            token_refresh_endpoint=token_refresh_endpoint,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )
        self.api_counter = api_counter
        self.redirect_uri = redirect_uri
        self.cycling_refresh_token = None

    def get_refresh_request_body(self) -> Mapping[str, Any]:
        payload = super().get_refresh_request_body()
        payload["redirect_uri"] = self.redirect_uri
        return payload

    def refresh_access_token(self) -> Tuple[str, int]:
        """
        returns a tuple of (access_token, token_lifespan_in_seconds)
        """
        try:
            response = requests.request(
                method="POST",
                url=self.token_refresh_endpoint,
                data=self.get_refresh_request_body(),
                headers=self.get_refresh_access_token_headers(),
            )
            self.api_counter.increment()
            response.raise_for_status()
            response_json = response.json()
            self.cycling_refresh_token = response_json["refresh_token"]
            self.refresh_token = self.cycling_refresh_token
            return response_json["access_token"], response_json["expires_in"]
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e


# Source
class SourceOutreach(AbstractSource):
    def __init__(self):
        self._api_counter = Counter()

    def _create_authenticator(self, config) -> OutreachAuthenticator:
        return OutreachAuthenticator(
            redirect_uri=config["redirect_uri"],
            token_refresh_endpoint=_TOKEN_REFRESH_ENDPOINT,
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"],
            api_counter=self._api_counter,
        )

    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            access_token, _ = self._create_authenticator(config).refresh_access_token()
            response = requests.get(
                _URL_BASE, headers={"Authorization": f"Bearer {access_token}"}
            )
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
            Prospects(**args),
            Sequences(**args),
            SequenceStates(**args),
            Events(**args),
            Templates(**args),
        ]
