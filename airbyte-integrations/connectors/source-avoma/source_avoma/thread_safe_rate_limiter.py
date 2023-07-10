from threading import Lock
from time import sleep, time

from .thread_safe_counter import Counter


class RequestRateLimiter:
    def __init__(self, requests_per_rate_limit: int, seconds_per_rate_limit: int):
        self._requests_per_rate_limit = requests_per_rate_limit
        self._seconds_per_rate_limit = seconds_per_rate_limit
        self._counter = Counter()
        self._reset_time = 0
        self._reset_request_count = self._counter.value()
        self._lock = Lock()

    def request_made_successfully(self):
        current_time = time()

        with self._lock:
            if int(current_time - self._reset_time) >= self._seconds_per_rate_limit:
                self._reset_time = int(current_time)
                self._reset_request_count = self._counter.value()

        self._counter.increment()

    def wait_if_needed(self):
        current_time = time()

        with self._lock:
            if self._reset_time != 0 and int(current_time - self._reset_time) >= self._seconds_per_rate_limit:
                self._reset_time = int(current_time)
                self._reset_request_count = self._counter.value()

                return

            requests_since_reset = self._counter.value() - self._reset_request_count
            if requests_since_reset >= self._requests_per_rate_limit:
                time_in_current_window = current_time - self._reset_time
                time_remaining_in_window = self._seconds_per_rate_limit - time_in_current_window
                sleep(time_remaining_in_window)

                self._reset_time = int(time())
                self._reset_request_count = self._counter.value()
