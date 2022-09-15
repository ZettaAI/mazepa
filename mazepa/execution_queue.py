from __future__ import annotations

from typing import Protocol, Iterable, runtime_checkable
import attrs
from .task import Task


@runtime_checkable
class ExecutionQueue(Protocol):
    def purge(self):
        ...

    def push_tasks(self, tasks: Iterable[Task]):
        ...

    def pull_completed_task_ids(self) -> Iterable[str]:
        ...


@attrs.mutable
class LocalExecutionQueue:
    completed_task_ids: list[str] = attrs.field(init=False, factory=list)

    def purge(self):
        pass

    def push_tasks(self, tasks: Iterable[Task]):
        for e in tasks:
            e()
            self.completed_task_ids.append(e.id_)

    def pull_completed_task_ids(self) -> Iterable[str]:
        result = self.completed_task_ids
        self.completed_task_ids = []
        return result
