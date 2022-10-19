from __future__ import annotations

from typing import TypeVar, Protocol, Type
from typing_extensions import ParamSpec

R_co = TypeVar("R_co", covariant=True)
P = ParamSpec("P")
P1 = ParamSpec("P1")


class CallableType(Protocol[P, P1, R_co]):  # pragma: no cover # protocol
    def __init__(self, *args: P.args, **kwargs: P.kwargs):
        pass

    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> R_co:
        pass
