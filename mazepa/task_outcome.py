from typing import Optional, TypeVar, Generic
from enum import Enum, unique, auto
import attrs


@unique
class TaskStatus(Enum):
    NOT_SUBMITTED = auto()
    SUBMITTED = auto()
    SUCCEEDED = auto()
    FAILED = auto()


R = TypeVar("R")


@attrs.mutable
class TaskOutcome(Generic[R]):
    status: TaskStatus
    exception: Optional[Exception] = None
    execution_secs: Optional[float] = None
    return_value: Optional[R] = None
