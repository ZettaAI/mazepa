from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable, Optional, List, Dict, Set
from collections import defaultdict
import attrs
from typeguard import typechecked

from .flows import Flow
from .tasks import Task
from .task_outcome import TaskOutcome, TaskStatus
from .dependency import Dependency


@runtime_checkable
class ExecutionState(Protocol):  # pragma: no cover
    def __init__(self, ongoing_flows: dict[str, Flow], completed_ids: Iterable[str] = ...):
        ...

    def get_ongoing_flow_ids(self) -> list[str]:
        ...

    def update_with_task_outcomes(self, task_outcomes: dict[str, TaskOutcome]):
        ...

    def get_task_batch(self, max_batch_len: int = ...) -> list[Task]:
        ...


@typechecked
@attrs.mutable
class InMemoryExecutionState:
    """
    ``ExecutionState`` implementation that keeps progress and dependency information
    as in-memory data structures.
    """

    ongoing_flows: Dict[str, Flow] = attrs.field(converter=lambda x: {e.id_: e for e in x})
    ongoing_exhausted_flow_ids: Set[str] = attrs.field(factory=set)
    ongoing_parent_map: Dict[str, Optional[str]] = attrs.field(
        init=False, default=defaultdict(lambda: None)
    )
    ongoing_children_map: Dict[str, Set[str]] = attrs.field(
        init=False, factory=lambda: defaultdict(set)
    )
    ongoing_tasks: Dict[str, Task] = attrs.field(factory=dict)

    completed_ids: Set[str] = attrs.field(factory=set)
    dependency_map: Dict[str, Set[str]] = attrs.field(init=False, factory=lambda: defaultdict(set))

    def get_ongoing_flow_ids(self) -> List[str]:
        """
        Return ids of the flows that haven't been completed.
        """
        return list(self.ongoing_flows.keys())

    def update_with_task_outcomes(self, task_outcomes: Dict[str, TaskOutcome]):
        """
        Given a mapping from tasks ids to task outcomes, update dependency and state of the
        execution. If any of the task outcomes indicates failure, will raise exception specified
        in the task outcome.

        :param task_ids: IDs of tasks indicated as completed.
        """

        for task_id, outcome in task_outcomes.items():
            if outcome.status == TaskStatus.FAILED:
                if outcome.exception is None:
                    outcome.exception = Exception(
                        "Task outcome of '{task_id}' indicated failure "
                        "without an exception specified."
                    )
                raise outcome.exception
            assert outcome.status == TaskStatus.SUCCEEDED

            if task_id in self.ongoing_tasks:
                self.ongoing_tasks[task_id].outcome = outcome
                self._update_completed_id(task_id)

    def get_task_batch(self, max_batch_len: int = 10000) -> List[Task]:
        """
        Generate the next batch of tasks that are ready for execution.

        :param max_batch_len: size limit after which no more flows will be querries for
            additional tasks. Note that the return length might be larger than
            ``max_batch_len``, as individual flows batches may not be subdivided.
        """

        result = []  # type: List[Task]
        for flow in list(self.ongoing_flows.values()):
            while (
                flow.id_ in self.ongoing_flows
                and len(self.dependency_map[flow.id_]) == 0
                and len(result) < max_batch_len
                and flow.id_ not in self.ongoing_exhausted_flow_ids
            ):
                flow_batch = self._get_batch_from_flow(flow)
                result.extend(flow_batch)

            if len(result) >= max_batch_len:
                break

        for e in result:
            self.ongoing_tasks[e.id_] = e

        return result

    def _add_dependency(self, flow_id: str, dep: Dependency):
        if dep.is_barrier():  # depend on all ongoing children
            self.dependency_map[flow_id].update(self.ongoing_children_map[flow_id])
        else:
            for id_ in dep.ids:
                if id_ not in self.completed_ids:
                    assert (
                        id_ in self.ongoing_children_map[flow_id]
                    ), f"Dependency on a non-child '{id_}' for flows '{flow_id}'"

                    self.dependency_map[flow_id].add(id_)

    def _update_completed_id(self, id_: str):
        self.completed_ids.add(id_)
        self.ongoing_exhausted_flow_ids.discard(id_)
        self.ongoing_flows.pop(id_, None)
        self.ongoing_tasks.pop(id_, None)

        parent_id = self.ongoing_parent_map[id_]
        if parent_id is not None:
            self.ongoing_children_map[parent_id].discard(id_)
            self.dependency_map[parent_id].discard(id_)
            if (
                parent_id in self.ongoing_exhausted_flow_ids
                and len(self.dependency_map[parent_id]) == 0
            ):
                self._update_completed_id(parent_id)

    def _get_batch_from_flow(self, flow):
        flow_yield = flow.get_next_batch()
        result = []
        if flow_yield is None:  # Means the flows is exhausted
            self.ongoing_exhausted_flow_ids.add(flow.id_)
            self.dependency_map[flow.id_].update(self.ongoing_children_map[flow.id_])
            if len(self.dependency_map[flow.id_]) == 0:
                self._update_completed_id(flow.id_)
        elif isinstance(flow_yield, Dependency):
            self._add_dependency(flow.id_, flow_yield)
        else:
            for e in flow_yield:
                if e.id_ not in self.completed_ids:
                    self.ongoing_children_map[flow.id_].add(e.id_)
                    self.ongoing_parent_map[e.id_] = flow.id_
                    if isinstance(e, Flow):
                        self.ongoing_flows[e.id_] = e
                    else:
                        assert isinstance(e, Task), "Typechecking error."
                        result.append(e)

        return result
