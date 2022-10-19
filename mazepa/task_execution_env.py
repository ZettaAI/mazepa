from __future__ import annotations

from typing import Optional
import attrs


@attrs.mutable
class TaskExecutionEnv:
    tags: set[str] = attrs.field(factory=set, converter=set)
    docker_image: Optional[str] = None

    def extend(self, other: TaskExecutionEnv):
        raise NotImplementedError  # pragma: no cover

    def apply_defaults(self, other: TaskExecutionEnv):
        raise NotImplementedError  # pragma: no cover
