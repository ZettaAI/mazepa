from __future__ import annotations

import time
import uuid
from typing import Callable, TypeVar, Generic, cast, Optional
import functools
from typing_extensions import ParamSpec
import attrs
from . import id_generators
from .task_outcome import TaskOutcome, TaskStatus
from .task_execution_env import TaskExecutionEnv

R = TypeVar("R")
P = ParamSpec("P")

# TODO: is tehre a better way?
NOT_EXECUTED = object()
UNKNOWN_RESULT = object()


@attrs.mutable
class TaskMaker(Generic[P, R]):
    """
    Wraps a function with to be executable by Mazepa scheduler.
    """

    fn: Callable[P, R]
    id_fn: Callable[[Callable, dict], str] = attrs.field(
        init=False, default=id_generators.get_unique_id
    )
    task_execution_env: TaskExecutionEnv = attrs.field(factory=TaskExecutionEnv)
    # max_retry: # Even for SQS, can use approximateReceiveCount to explicitly fail the task

    def __call__(
        self: "TaskMaker[P, R]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:  # pragma: no cover
        return self.fn(*args, **kwargs)

    def make_task(
        self: "TaskMaker[P, R]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R]:
        # TODO: allow adjustments to the task execution environment here
        assert len(args) == 0, "Tasks must use keyword arguments only"
        id_ = self.id_fn(self.fn, kwargs)
        result = Task[P, R](
            fn=self.fn,
            kwargs=kwargs,
            id_=id_,
        )
        return result


@attrs.mutable
class Task(Generic[P, R]):
    """
    An executable task.
    """

    fn: Callable[P, R]
    kwargs: dict = {}
    id_: str = attrs.field(factory=lambda: str(uuid.uuid1()))
    task_execution_env: TaskExecutionEnv = attrs.field(factory=TaskExecutionEnv)

    _mazepa_callbacks: list[Callable] = attrs.field(factory=list)
    outcome: TaskOutcome = attrs.field(
        factory=functools.partial(
            TaskOutcome,
            status=TaskStatus.NOT_SUBMITTED,
        )
    )
    # cache_expiration: datetime.timedelta = None
    # max_retry: # Can use SQS approximateReceiveCount to explicitly fail the task

    def __call__(self: "Task[P, R]") -> TaskOutcome[Optional[R]]:
        time_start = time.time()
        try:
            # TODO: parametrize by task execution environment
            return_value = self.fn(**self.kwargs)
            status = TaskStatus.SUCCEEDED
            exception = None
        # Todo: catch special exceptions
        except Exception as exc:  # pylint: disable=broad-except
            exception = exc
            return_value = None
            status = TaskStatus.FAILED

        time_end = time.time()

        self.outcome = TaskOutcome(
            status=status,
            exception=exception,
            execution_secs=time_end - time_start,
            return_value=return_value,
        )
        for callback in self._mazepa_callbacks:
            callback(task=self)
        return self.outcome


def task_maker(
    fn: Callable[P, R],
) -> TaskMaker[P, R]:
    return cast(
        TaskMaker[P, R],
        TaskMaker(fn=fn),
    )
