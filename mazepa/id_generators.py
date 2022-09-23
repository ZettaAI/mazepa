from typing import Callable
import uuid


def get_unique_id(
    fn: Callable, kwargs: dict
) -> str:  # pragma: no cover  # pylint: disable=unused-argument
    return str(uuid.uuid1())
