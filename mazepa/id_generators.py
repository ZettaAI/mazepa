from typing import Callable
import uuid


def get_unique_id(fn: Callable, kwargs: dict) -> str:  # pylint: disable=unused-argument
    return str(uuid.uuid1())
