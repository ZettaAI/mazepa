from typing import Optional, Iterable
import time
from collections import defaultdict
import attrs

from .queue import Queue
from .job import Job
from .task import Task
from .waiting import Await

from abc import ABC, abstractmethod

class Scheduler(ABC):
    @abstractmethod
    def add_job(self, job: Job):
        ...

    @abstractmethod
    def execute(self):
        ...

@attrs.mutable
class LocalScheduler(Scheduler):
    pass

@attrs.mutable
class QueueScheduler(Scheduler):
    queue: Queue
    batch_gap_sec: float = 4
    ongoing_jobs: dict[str, Job] = attrs.field(factory=dict)
    finished_jobs: dict[str, Job] = attrs.field(init=False, factory=dict)
    job_task_children: dict[str, set[str]] = attrs.field(init=False, factory=lambda: defaultdict(set))
    job_awaits: dict[str, Optional[Await]] = attrs.field(init=False, factory=lambda: defaultdict(lambda: None))
    #job_subjob_children: dict[str, set[str]] = attrs.field(init=False, factory=defaultdict(set))
    finished_task_ids: set[str] = attrs.field(init=False, factory=set)

    def add_job(self, job: Job):
        self.ongoing_jobs[job.id_] = job

    def execute(self):
        while True:
            ready_job_ids = self._get_ready_job_ids()
            self._submit_tasks_for_jobs(ready_job_ids)
            if len(self.ongoing_jobs) == 0:
                break
            time.sleep(self.batch_gap_sec)

    def _update_finished_ids(self):
        new_completed_tasks = self.queue.get_new_completed_task_ids()
        self.finished_task_ids.update(new_completed_tasks)

        for job_id, waiting_for in self.job_awaits:


    def _get_ready_job_ids(self) -> Iterable[str]:
        self._update_finished_ids()
        for job_id, job in self.ongoing_jobs.items():

        return []

    def _submit_tasks_for_jobs(self, job_ids: Iterable[str] = None):
        jobs_just_finished = set() # type: set[str]
        common_task_batch  = [] # type: list[Task]

        for job_id in job_ids:
            job = self.ongoing_jobs[job_id]
            job_yield = job.get_next_batch()

            if job_yield is None:
                jobs_just_finished.add(job_id)
            elif isinstance(job_yield, Await):
                pass
            else:
                if isinstance(job_yield, list):
                    children_batch = job_yield
                else:
                    children_batch = [job_yield] # type: ignore # lists are uniform type

                if isinstance(children_batch[0], Task):
                    for e in children_batch:
                        self.job_task_children[job_id].add(e.id_)
                    common_task_batch += children_batch # type: ignore # all tasks here
                else:
                    assert isinstance(children_batch[0], Job), "Typechecking error."
