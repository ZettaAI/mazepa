from typing import Optional, Iterable

import attrs
from typeguard import typechecked


@typechecked
@attrs.frozen
class Dependency:
    job_ids: Optional[Iterable[str]] = None
    task_ids: Optional[Iterable[str]] = None

    def is_barrier(self) -> bool:
        return self.job_ids is None and self.task_ids is None
