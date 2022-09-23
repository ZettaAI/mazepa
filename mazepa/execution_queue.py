from __future__ import annotations

from typing import Protocol, Iterable, runtime_checkable, Dict
from typeguard import typechecked
import attrs
from .task import Task
from .task_outcome import TaskOutcome


@runtime_checkable
class ExecutionQueue(Protocol): # pragma: no cover
    name: str

    def purge(self):
        ...

    def push_tasks(self, tasks: Iterable[Task]):
        ...

    def pull_task_outcomes(self) -> Dict[str, TaskOutcome]:
        ...


@typechecked
@attrs.mutable
class LocalExecutionQueue:
    task_outcomes: Dict[str, TaskOutcome] = attrs.field(init=False, factory=dict)

    def purge(self): # pragma: no cover
        pass

    def push_tasks(self, tasks: Iterable[Task]):
        for e in tasks:
            e()
            self.task_outcomes[e.id_] = e.outcome

    def pull_task_outcomes(self) -> Dict[str, TaskOutcome]:
        result = self.task_outcomes
        self.task_outcomes = {}
        return result
