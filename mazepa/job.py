from __future__ import annotations

from typing import Callable, TypeVar, Generator, Union, Any, Optional, Iterable, List
from contextlib import contextmanager
import uuid
from typing_extensions import ParamSpec
import attrs
from typeguard import typechecked
from .task import Task
from .dependency import Dependency
from .task_execution_env import TaskExecutionEnv


BatchType = Optional[Union[Dependency, List[Task], List["Job"]]]
JobFnYieldType = Union[Dependency, "Task", List["Task"], "Job", List["Job"]]
JobFnReturnType = Generator[JobFnYieldType, None, Any]
R = TypeVar("R", bound=JobFnReturnType)
P = ParamSpec("P")

JOB_EXHAUSTED = object()


@typechecked
@attrs.mutable
class Job:
    """
    Wraps a function to become a job that can be registered with Mazepa scheduler.
    """

    fn: Callable[..., JobFnReturnType]
    args: Iterable = attrs.field(factory=list)
    kwargs: dict = attrs.field(factory=dict)
    id_: str = attrs.field(factory=lambda: f"job-{str(uuid.uuid1())}")
    task_execution_env: Optional[TaskExecutionEnv] = attrs.field(default=None)
    _iterator: Optional[JobFnReturnType] = attrs.field(init=False, default=None)

    @contextmanager
    def task_execution_env_ctx(self, env: Optional[TaskExecutionEnv]):
        old_env = self.task_execution_env
        self.task_execution_env = env
        yield
        self.task_execution_env = old_env

    def get_next_batch(self) -> BatchType:
        if self._iterator is None:
            self._iterator = self.fn(*self.args, **self.kwargs)
        yielded = next(self._iterator, None)

        result: BatchType
        if isinstance(yielded, Job):
            result = [yielded]
        elif isinstance(yielded, Task):
            result = [yielded]
        else:
            result = yielded

        if self.task_execution_env is not None and isinstance(result, list):
            for e in result:
                e.task_execution_env = self.task_execution_env

        return result


def job(fn: Callable[P, JobFnReturnType]) -> Callable[..., Job]:  # pragma: no cover
    def wrapped(*args: P.args, **kwargs: P.kwargs):
        return Job(fn=fn, args=args, kwargs=kwargs)

    return wrapped
