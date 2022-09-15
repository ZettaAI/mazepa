from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable, Optional
from collections import defaultdict
import attrs

from .job import Job
from .task import Task
from .dependency import Dependency


@runtime_checkable
class ExecutionState(Protocol):  # pragma: no cover
    def __init__(self, ongoing_jobs: dict[str, Job], completed_ids: Iterable[str] = ...):
        ...

    def get_ongoing_job_ids(self) -> list[str]:
        ...

    def update_completed_task_ids(self, task_ids: Iterable[str]):
        ...

    def get_task_batch(self, max_batch_len: int = ...) -> list[Task]:
        ...


@attrs.mutable
class InMemoryExecutionState:
    """
    ``ExecutionState`` implementation that keeps progress and dependency information
    as in-memory data structures.
    """

    ongoing_jobs: dict[str, Job] = attrs.field(converter=lambda x: {e.id_: e for e in x})
    ongoing_exhausted_job_ids: set[str] = attrs.field(factory=set)
    completed_ids: set[str] = attrs.field(factory=set)

    ongoing_parent_map: dict[str, Optional[str]] = attrs.field(
        init=False, default=defaultdict(lambda: None)
    )
    ongoing_children_map: dict[str, set[str]] = attrs.field(
        init=False, factory=lambda: defaultdict(set)
    )
    dependency_map: dict[str, set[str]] = attrs.field(init=False, factory=lambda: defaultdict(set))
    # job_subjob_children: dict[str, set[str]] = attrs.field(init=False, factory=defaultdict(set))

    def get_ongoing_job_ids(self) -> list[str]:
        """
        Return ids of the tasks that haven't been completed.
        """
        return list(self.ongoing_jobs.keys())

    def update_completed_task_ids(self, task_ids: Iterable[str]):
        """
        Given a list of completed task ids, update dependency state of the execution.

        :param task_ids: IDs of tasks indicated as completed.
        """
        for task_id in task_ids:
            self._update_completed_id(task_id)

    def get_task_batch(self, max_batch_len: int = 10000) -> list[Task]:
        """
        Generate the next batch of tasks that are ready for execution.

        :param max_batch_len: size limit after which no more jobs will be querries for
            additional tasks. Note that the return length might be larger than
            ``max_batch_len``, as individual job batches may not be subdivided.
        """

        result = []  # type: list[Task]
        for job in list(self.ongoing_jobs.values()):
            while (
                len(self.dependency_map[job.id_]) == 0
                and len(result) < max_batch_len
                and job.id_ not in self.ongoing_exhausted_job_ids
            ):
                job_batch = self._get_batch_from_job(job)
                result.extend(job_batch)

            if len(result) >= max_batch_len:
                break

        return result

    def _add_dependency(self, job_id: str, dep: Dependency):
        if dep.is_barrier():  # depend on all ongoing children
            self.dependency_map[job_id].update(self.ongoing_children_map[job_id])
        else:
            for id_ in dep.ids:
                if id_ not in self.completed_ids:
                    assert id_ in self.ongoing_children_map[job_id], f"Dependency on a non-child '{id_}' for job '{job_id}'"
                    self.dependency_map[job_id].add(id_)

    def _update_completed_id(self, id_: str):
        print (self.dependency_map)
        print (self.ongoing_children_map)
        self.ongoing_exhausted_job_ids.discard(id_)
        self.completed_ids.add(id_)
        self.ongoing_jobs.pop(id_, None)


        parent_id = self.ongoing_parent_map[id_]
        if parent_id is not None :
            if id_ in self.ongoing_children_map[parent_id]:
                self.ongoing_children_map[parent_id].remove(id_)
            if id_ in self.dependency_map[parent_id]:
                self.dependency_map[parent_id].remove(id_)
                if (
                    parent_id in self.ongoing_exhausted_job_ids
                    and len(self.dependency_map[parent_id]) == 0
                ):
                    self._update_completed_id(parent_id)

    def _get_batch_from_job(self, job):
        job_yield = job.get_next_batch()
        result = []
        if job_yield is None:  # Means the job is exhausted
            self.ongoing_exhausted_job_ids.add(job.id_)
            self.dependency_map[job.id_].update(self.ongoing_children_map[job.id_])
            # self.completed_ids.add(job.id_)
            # del self.ongoing_jobs[job.id_]
        elif isinstance(job_yield, Dependency):
            self._add_dependency(job.id_, job_yield)
        else:
            for e in job_yield:
                if e.id_ not in self.completed_ids:
                    self.ongoing_children_map[job.id_].add(e.id_)
                    self.ongoing_parent_map[e.id_] = job.id_
                    if isinstance(e, Job):
                        self.ongoing_jobs[e.id_] = e
                    else:
                        assert isinstance(e, Task), "Typechecking error."
                        result.append(e)

        return result
