"""Preserve process queue feeder failures for parent-side checkpoint checks."""

import multiprocessing as mp
from multiprocessing.queues import Queue
from typing import Any


class ProcessJobQueue(Queue):
    def __init__(self) -> None:
        super().__init__(ctx=mp.get_context())
        self.feeder_error: Exception | None = None

    def __setstate__(self, state: Any) -> None:
        super().__setstate__(state)
        self.feeder_error = None

    def _on_queue_feeder_error(self, error: Exception, obj: Any) -> None:
        if self.feeder_error is None:
            self.feeder_error = error
        super()._on_queue_feeder_error(error, obj)  # type: ignore[misc]  # CPython feeder hook.
