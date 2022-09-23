from __future__ import annotations

from typing import Optional
import attrs

@attrs.mutable
class TaskExecutionEnv:
    queue_tags: set[str] = attrs.field(factory=set, converter=set)
    docker_image: Optional[str] = None

    def extend(self, other: TaskExecutionEnv):
        raise NotImplementedError

    def apply_defaults(self, other: TaskExecutionEnv):
        raise NotImplementedError
