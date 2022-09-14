from typing import Callable, TypeVar, Generic, cast
import uuid
from typing_extensions import ParamSpec
import attrs

R = TypeVar("R")  # The return type of the user's function
P = ParamSpec("P")  # The parameters of the task

NOT_EXECUTED = object()


class Task(Generic[P, R]):
    """
    Wraps a function with to be executable by Mazepa scheduler.
    """

    fn: Callable[P, R]
    id_: str = attrs.field(init=False, factory=lambda: str(uuid.uuid1))
    callbacks: list[Callable] = attrs.field(factory=list)
    result: NOT_EXECUTED

    # tags: Iterable[str] = None
    # cache_key_fn
    # cache_expiration: datetime.timedelta = None
    # max_retry: # Can use SQS approximateReceiveCount to explicitly fail the task

    def __call__(
        self: "Task[P, R]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        result = self.fn(*args, **kwargs)
        for callback in self.callbacks:
            callback(
                task=self,
                result=result,
            )
        return result


def task(fn: Callable[P, R]) -> Task[P, R]:
    return cast(
        Task[P, R],
        Task(fn=fn),
    )
