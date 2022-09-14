from __future__ import annotations

from typing import Callable, TypeVar, Generic, Generator, Union, Any, Optional
import uuid
from typing_extensions import ParamSpec
import attrs
from typeguard import typechecked
from .task import Task
from .waiting import Await

JobFnYieldType = Union[None, Await, Task, list[Task], "Job", list["Job"]]
JobFnReturnType = Generator[JobFnYieldType, None, Any]
R = TypeVar("R", bound=JobFnReturnType)
P = ParamSpec("P")

@typechecked
@attrs.mutable
class Job(Generic[P]):
    """
    Wraps a function to become a job that can be registered with Mazepa scheduler.
    """

    fn: Callable[P, JobFnReturnType]
    id_: str = attrs.field(init=False, factory=lambda: f"job-{str(uuid.uuid1)}")
    iterator: Optional[JobFnReturnType] = attrs.field(init=False, default=None)

    def get_next_batch(
        self: "Job[P]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> JobFnYieldType:
        if self.iterator is None:
            self.iterator = self.fn(*args, **kwargs)
        result = next(self.iterator)
        return result


@typechecked
def job(fn: Callable[P, JobFnReturnType]) -> Job[P]:
    return Job(fn=fn)
