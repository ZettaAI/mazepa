from __future__ import annotations

from typing import Callable, TypeVar, Generator, Union, Any, Optional, Iterable, List
import uuid
from typing_extensions import ParamSpec
import attrs
from typeguard import typechecked
from .task import Task
from .dependency import Dependency


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
    iterator: Optional[JobFnReturnType] = attrs.field(init=False, default=None)
    args: Iterable = attrs.field(factory=list)
    kwargs: dict = attrs.field(factory=dict)
    id_: str = attrs.field(factory=lambda: f"job-{str(uuid.uuid1())}")

    def get_next_batch(self) -> BatchType:
        if self.iterator is None:
            self.iterator = self.fn(*self.args, **self.kwargs)
        yielded = next(self.iterator, None)

        result: BatchType
        if isinstance(yielded, Job):
            result = [yielded]
        elif isinstance(yielded, Task):
            result = [yielded]
        else:
            result = yielded
        return result


@typechecked
def job(fn: Callable[P, JobFnReturnType]) -> Callable[..., Job]:
    def wrapped(*args: P.args, **kwargs: P.kwargs):
        return Job(fn=fn, args=args, kwargs=kwargs)

    return wrapped
