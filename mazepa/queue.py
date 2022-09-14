from typing import Protocol
from .task import Task


class Queue(Protocol):
    def purge(self, tasks: list[Task]):
        ...

    def submit(self, tasks: list[Task]):
        ...

    def get_new_completed_task_ids(self) -> list[str]:
        ...
