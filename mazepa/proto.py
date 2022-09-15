from typing import Iterable, Any
from typing_extensions import Protocol


class SupportsClose(Protocol):
    def close(self) -> None:
        ...


class Resource:  # No SupportsClose base class!
    resource: Any

    def close(self) -> None:
        self.resource.release()


def close_all(items: Iterable[SupportsClose]) -> None:
    for item in items:
        item.close()


close_all(
    [
        Resource(),
        Resource(),
    ]
)  # Okay!
