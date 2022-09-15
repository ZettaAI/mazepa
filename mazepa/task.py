from __future__ import annotations

from typing import Callable, TypeVar, Generic, cast, Any

from typing_extensions import ParamSpec
import attrs
from . import id_generators

R = TypeVar("R")  # The return type of the user's function
P = ParamSpec("P")  # The parameters of the task

NOT_EXECUTED = object()


@attrs.mutable
class TaskMaker(Generic[P, R]):
    """
    Wraps a function with to be executable by Mazepa scheduler.
    """

    fn: Callable[P, R]
    id_fn: Callable[[Callable, dict], str] = attrs.field(
        init=False, default=id_generators.get_unique_id
    )
    tags: list[str] = attrs.field(factory=list)
    # max_retry: # Can use SQS approximateReceiveCount to explicitly fail the task

    def __call__(
        self: "TaskMaker[P, R]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        return self.fn(*args, **kwargs)

    def make_task(
        self: "TaskMaker[P, R]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R]:
        assert len(args) == 0
        id_ = self.id_fn(self.fn, kwargs)
        result = Task[P, R](
            fn=self.fn,
            kwargs=kwargs,
            id_=id_,
            tags=self.tags,
        )
        return result


@attrs.mutable
class Task(Generic[P, R]):
    """
    An executable task with ID and tags.
    """

    fn: Callable[P, R]
    kwargs: dict
    id_: str

    callbacks: list[Callable] = attrs.field(factory=list)
    result: Any = NOT_EXECUTED
    tags: list[str] = attrs.field(factory=list)
    # cache_expiration: datetime.timedelta = None
    # max_retry: # Can use SQS approximateReceiveCount to explicitly fail the task

    def __call__(self: "Task[P, R]") -> R:
        self.result = self.fn(**self.kwargs)
        for callback in self.callbacks:
            callback(task=self)
        return self.result


def task_maker(
    fn: Callable[P, R],
) -> TaskMaker[P, R]:

    return cast(
        TaskMaker[P, R],
        TaskMaker(fn=fn),
    )
