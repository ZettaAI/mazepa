from __future__ import annotations
import time
from collections import defaultdict
from typing import Protocol, Iterable, runtime_checkable, Dict, List
from typeguard import typechecked
import attrs
from zetta_utils.log import get_logger
from .tasks import Task
from .task_outcome import TaskOutcome

logger = get_logger("mazepa")


@runtime_checkable
class ExecutionQueue(Protocol):  # pragma: no cover
    name: str

    def purge(self):
        ...

    def push_tasks(self, tasks: Iterable[Task]):
        ...

    def pull_task_outcomes(
        self,
        max_num: int = ...,
    ) -> dict[str, TaskOutcome]:
        ...

    def pull_tasks(self, max_num: int = ...) -> List[Task]:
        ...


@typechecked
@attrs.mutable
class LocalExecutionQueue:
    name: str = "local_execution"
    task_outcomes: Dict[str, TaskOutcome] = attrs.field(init=False, factory=dict)

    def purge(self):  # pragma: no cover
        pass

    def push_tasks(self, tasks: Iterable[Task]):
        for e in tasks:
            logger.debug(f"STARTING: Execution of {e}.")
            e()
            logger.debug(f"DONE: Execution of {e}.")
            self.task_outcomes[e.id_] = e.outcome

    def pull_task_outcomes(
        self, max_num: int = 100000, max_time_sec: float = 2.5  # pylint: disable=unused-argument
    ) -> Dict[str, TaskOutcome]:
        outcome_items = list(self.task_outcomes.items())
        return_num = min(max_num, len(self.task_outcomes))
        result = dict(outcome_items[:return_num])
        self.task_outcomes = dict(outcome_items[return_num:])
        return result

    def pull_tasks(  # pylint: disable=no-self-use
        self, max_num: int = 1  # pylint: disable=unused-argument
    ) -> list[Task]:  # pragma: no cover
        return []


@typechecked
@attrs.frozen
class ExecutionMultiQueue:
    name: str = attrs.field(init=False)
    queues: Iterable[ExecutionQueue]

    def __attrs_post_init__(self):
        name = "_".join(queue.name for queue in self.queues)
        object.__setattr__(self, "name", name)

    def purge(self):
        for e in self.queues:
            e.purge()
            logger.debug(f"Purged {e}.")

    def push_tasks(self, tasks: Iterable[Task]):
        tasks_for_queue = defaultdict(list)

        for task in tasks:
            matching_queue_names = [
                queue.name
                for queue in self.queues
                if all(tag in queue.name for tag in task.task_execution_env.tags)
            ]
            if len(matching_queue_names) == 0:
                raise RuntimeError(
                    f"No queue from set {list(self.queues)} matches "
                    f"all tags {task.task_execution_env.tags}."
                )
            tasks_for_queue[matching_queue_names[0]].append(task)

        for queue in self.queues:
            queue.push_tasks(tasks_for_queue[queue.name])

    def pull_task_outcomes(
        self, max_num: int = 500, max_time_sec: float = 2.5
    ) -> Dict[str, TaskOutcome]:
        start_ts = time.time()
        result = {}  # type: dict[str, TaskOutcome]
        for queue in self.queues:
            queue_outcomes = queue.pull_task_outcomes(max_num=max_num - len(result))
            result = {**result, **queue_outcomes}
            if len(result) >= max_num:
                break
            now_ts = time.time()
            if now_ts - start_ts >= max_time_sec:
                break

        return result

    def pull_tasks(self, max_num: int = 1) -> List[Task]:
        result = []  # type: list[Task]

        for queue in self.queues:
            result += queue.pull_tasks(max_num=max_num - len(result))
            if len(result) >= max_num:
                break

        return result
