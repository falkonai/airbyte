#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from typing import Optional

import requests
from airbyte_cdk.sources.streams.http.rate_limiting import default_backoff_handler

from .thread_safe_counter import Counter


class Outplay:
    def __init__(
        self,
        location: str = None,
        client_id: str = None,
        client_secret: str = None,
        is_sandbox: bool = None,
        start_date: str = None,
        api_type: str = None,
        max_api_requests: Optional[int] = None,
        api_counter: Optional[Counter] = None,
    ):
        self.api_type = api_type.upper() if api_type else None
        self.location = location
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.instance_url = None
        self.session = requests.Session()
        self.is_sandbox = is_sandbox is True or (isinstance(is_sandbox, str) and is_sandbox.lower() == "true")
        self.start_date = start_date
        self.api_counter = api_counter
        self.max_api_requests = max_api_requests if max_api_requests is not None else 100000

    def login(self):
        test_url = f"https://{self.location or 'us4'}-api.outplayhq.com/api/v1/me?client_id={self.client_id}"

        resp = self._make_request(
            "GET", test_url, body=None, headers={"Content-Type": "application/json", "X-CLIENT-SECRET": self.client_secret}
        )

        me = resp.json()
        self.account = me["account"]

    @default_backoff_handler(max_tries=5, factor=15)
    def _make_request(
        self,
        http_method: str,
        url: str,
        headers: dict = None,
        body: dict = None,
        stream: bool = False,
        params: dict = None,
    ) -> requests.models.Response:
        if http_method == "GET":
            resp = self.session.get(url, headers=headers, stream=stream, params=params)
        elif http_method == "POST":
            resp = self.session.post(url, headers=headers, data=body)
        if self.api_counter is not None:
            self.api_counter.increment()
        resp.raise_for_status()

        return resp
