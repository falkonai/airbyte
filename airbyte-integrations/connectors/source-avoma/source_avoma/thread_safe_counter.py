from threading import Lock


class Counter:
    def __init__(self, initial_value: int = 0):
        self._counter = initial_value
        self._lock = Lock()

    def increment(self):
        with self._lock:
            self._counter += 1

    def value(self):
        with self._lock:
            return self._counter
