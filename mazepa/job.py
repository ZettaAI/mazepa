from __future__ import annotations

from typing import (
    Callable,
    Generator,
    Union,
    Any,
    Optional,
    List,
    Generic,
    Iterable,
    Dict,
)
from contextlib import contextmanager
import uuid
from typing_extensions import ParamSpec
import attrs
from coolname import generate_slug  # type: ignore
from .task import Task
from .dependency import Dependency
from .task_execution_env import TaskExecutionEnv


BatchType = Optional[Union[Dependency, List[Task], List["Job"]]]
JobFnYieldType = Union[Dependency, "Task", List["Task"], "Job", List["Job"]]
JobFnReturnType = Generator[JobFnYieldType, None, Any]
P = ParamSpec("P")

JOB_EXHAUSTED = object()


@attrs.mutable(init=False)
class Job(Generic[P]):
    """
    Wraps a function to become a job that can be registered with Mazepa scheduler.
    """

    id_: str
    task_execution_env: Optional[TaskExecutionEnv]
    _iterator: JobFnReturnType
    fn: Callable[P, JobFnReturnType]
    # These are saved as attributes just for printability.
    args: Iterable
    kwargs: Dict

    # Disabling attrs init to use P Param Spec for args and kwargs.
    # Improvements wellcome
    def __init__(
        self,
        fn: Callable[P, JobFnReturnType],
        id_: str,
        task_execution_env: Optional[TaskExecutionEnv],
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.id_ = id_
        self.task_execution_env = task_execution_env
        self._iterator = fn(*args, **kwargs)

    @contextmanager
    def task_execution_env_ctx(self, env: Optional[TaskExecutionEnv]):
        old_env = self.task_execution_env
        self.task_execution_env = env
        yield
        self.task_execution_env = old_env

    def get_next_batch(self) -> BatchType:
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


def job(fn: Callable[P, JobFnReturnType]) -> Callable[P, Job[P, JobFnReturnType]]:
    def wrapped(*args: P.args, **kwargs: P.kwargs):
        if len(args) != 0:
            raise RuntimeError(
                "Only keyword arguments are allowed for mazepa jobs. "
                f"Got: args={args}, kwargs={kwargs}"
            )
        result = Job(
            fn=fn,
            id_=f"job-{generate_slug(2)}-{str(uuid.uuid1())}",
            task_execution_env=None,  # TODO
            **kwargs,
        )
        return result

    return wrapped
