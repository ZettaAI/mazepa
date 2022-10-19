from __future__ import annotations

import time
import uuid
from typing import (
    Callable,
    TypeVar,
    Generic,
    Iterable,
    Dict,
    Protocol,
    Type,
    runtime_checkable,
)
import functools
from typing_extensions import ParamSpec
import attrs
from . import id_generators
from .task_outcome import TaskOutcome, TaskStatus
from .task_execution_env import TaskExecutionEnv

R_co = TypeVar("R_co", covariant=True)
P = ParamSpec("P")
P1 = ParamSpec("P1")


@runtime_checkable
class Task(Protocol[P, R_co]):  # pragma: no cover # protocol
    """
    An executable task.
    """

    id_: str
    task_execution_env: TaskExecutionEnv

    _mazepa_callbacks: list[Callable]
    outcome: TaskOutcome

    def set_up(self, *args: P.args, **kwargs: P.kwargs):
        ...

    def __call__(self) -> TaskOutcome[R_co]:
        ...


@attrs.mutable
class _Task(Generic[P, R_co]):
    """
    An executable task.
    """

    fn: Callable[P, R_co]
    id_: str = attrs.field(factory=lambda: str(uuid.uuid1()))
    task_execution_env: TaskExecutionEnv = attrs.field(factory=TaskExecutionEnv)
    args_are_set: bool = attrs.field(init=False, default=False)
    args: Iterable = attrs.field(init=False, factory=list)
    kwargs: Dict = attrs.field(init=False, factory=dict)

    _mazepa_callbacks: list[Callable] = attrs.field(factory=list)
    outcome: TaskOutcome = attrs.field(
        factory=functools.partial(
            TaskOutcome,
            status=TaskStatus.NOT_SUBMITTED,
        )
    )
    # cache_expiration: datetime.timedelta = None
    # max_retry: # Can use SQS approximateReceiveCount to explicitly fail the task

    # Split into __init__ and set_up because ParamSpec doesn't allow us
    # to play with kwargs.
    # cc: https://peps.python.org/pep-0612/#concatenating-keyword-parameters
    def set_up(self, *args: P.args, **kwargs: P.kwargs):
        assert not self.args_are_set
        self.args = args
        self.kwargs = kwargs
        self.args_are_set = True

    def __call__(self) -> TaskOutcome[R_co]:
        assert self.args_are_set

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


@runtime_checkable
class TaskFactory(Protocol[P, R_co]):  # pragma: no cover # protocol
    """
    Wraps a callable to add a ``make_task`` method,
    ``make_task`` method creates a mazepa task corresponding to execution of
    the callable with the given parameters.
    """

    def __call__(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R_co:
        ...

    def make_task(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R_co]:
        ...


@attrs.mutable
class _TaskFactory(Generic[P, R_co]):
    """
    Implementation of the task factory.
    """

    fn: Callable[P, R_co]
    id_fn: Callable[[Callable, dict], str] = attrs.field(default=id_generators.get_unique_id)
    task_execution_env: TaskExecutionEnv = attrs.field(factory=TaskExecutionEnv)
    # max_retry: # Even for SQS, can use approximateReceiveCount to explicitly fail the task

    def __call__(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R_co:  # pragma: no cover
        return self.fn(*args, **kwargs)

    def make_task(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R_co]:
        id_ = self.id_fn(self.fn, kwargs)
        result = _Task[P, R_co](fn=self.fn, id_=id_, task_execution_env=self.task_execution_env)
        result.set_up(*args, **kwargs)  # pylint: disable=protected-access # friend class
        return result


def task_factory(fn: Callable[P, R_co]) -> TaskFactory[P, R_co]:
    return _TaskFactory(fn=fn)


def task_factory_cls(
    cls: Type[Callable[P, R_co]],
) -> Type[Callable[P, R_co]]:
    def _make_task(self, *args, **kwargs):
        return _TaskFactory(  # pylint: disable=protected-access
            self,
            # TODO: Other params passed to decorator
        ).make_task(
            *args, **kwargs
        )  # pylint: disable=protected-access

    # can't override __new__ because it doesn't combine well with attrs/dataclass
    setattr(cls, "make_task", _make_task)
    return cls
