from __future__ import annotations

import time
import uuid
from typing import Callable, TypeVar, Generic, Optional
import functools
from typing_extensions import ParamSpec
import attrs
from . import id_generators
from .task_outcome import TaskOutcome, TaskStatus
from .task_execution_env import TaskExecutionEnv

R_co = TypeVar("R_co", covariant=True)
P = ParamSpec("P")
P1 = ParamSpec("P1")

# TODO: is tehre a better way?
NOT_EXECUTED = object()
UNKNOWN_RESULT = object()


@attrs.mutable
class TaskMaker(Generic[P, R_co]):
    """
    Wraps a function to be executable by Mazepa scheduler.
    """

    fn: Callable[P, R_co]
    id_fn: Callable[[Callable, dict], str] = attrs.field(
        init=False, default=id_generators.get_unique_id
    )
    task_execution_env: TaskExecutionEnv = attrs.field(factory=TaskExecutionEnv)
    # max_retry: # Even for SQS, can use approximateReceiveCount to explicitly fail the task

    def __call__(
        self: TaskMaker[P, R_co],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R_co:  # pragma: no cover
        return self.fn(*args, **kwargs)

    def make_task(
        self: TaskMaker[P, R_co],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R_co]:
        # TODO: allow adjustments to the task execution environment here
        assert len(args) == 0, "Tasks must use keyword arguments only"
        id_ = self.id_fn(self.fn, kwargs)
        result = Task[P, R_co](
            fn=self.fn,
            kwargs=kwargs,
            id_=id_,
        )
        return result


@attrs.mutable
class Task(Generic[P, R_co]):
    """
    An executable task.
    """

    fn: Callable[P, R_co]
    kwargs: dict = {}  # TODO: Cannot use P.Kwargs here. SHould probably give up attrs
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

    def __call__(self: Task[P, R_co]) -> TaskOutcome[Optional[R_co]]:
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
    fn: Callable[P, R_co],
) -> TaskMaker[P, R_co]:
    return TaskMaker(fn=fn)


def _new_task_maker_cls(cls: Callable[P, Callable[P1, R_co]], *args: P.args, **kwargs: P.kwargs):
    obj = object.__new__(cls)  # type: ignore
    object.__setattr__(obj, "make_task", TaskMaker(obj.__call__).make_task)  # type: ignore
    obj.__init__(*args, **kwargs)  # type: ignore
    return obj


def task_maker_cls(cls: Callable[P, R_co]) -> Callable[P, R_co]:
    cls.__new__ = _new_task_maker_cls  # type: ignore
    return cls
