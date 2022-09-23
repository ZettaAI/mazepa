from __future__ import annotations

from typing import Optional, Iterable, Union, Callable
import time

from .execution_queue import ExecutionQueue, LocalExecutionQueue
from .job import Job
from .execution_state import ExecutionState, InMemoryExecutionState


def execute(
    target: Union[Job, Iterable[Job], ExecutionState],
    exec_queue: Optional[ExecutionQueue] = None,
    batch_gap_sleep_sec: float = 4.0,
    purge_at_start: bool = False,
    max_batch_len: int = 10000,
    default_state_constructor: Callable[..., ExecutionState] = InMemoryExecutionState,
):
    """
    Executes a target until completion using the given execution queue.
    Execution is performed by making an execution state from the target and passing new task
    batches and completed task ids between the state and the execution queue.
    """

    if isinstance(target, ExecutionState):
        state = target
    else:
        if not isinstance(target, Job):
            jobs = target
        else:
            jobs = [target]
        state = default_state_constructor(ongoing_jobs=jobs)

    if exec_queue is None:
        queue = LocalExecutionQueue()  # type: ExecutionQueue
    else:
        queue = exec_queue

    if purge_at_start:
        queue.purge()

    while True:
        if len(state.get_ongoing_job_ids()) == 0:
            break

        task_batch = state.get_task_batch(max_batch_len=max_batch_len)
        queue.push_tasks(task_batch)
        time.sleep(batch_gap_sleep_sec)
        task_outcomes = queue.pull_task_outcomes()
        state.update_with_task_outcomes(task_outcomes)
