import time
import taskqueue  # type: ignore

from . import serialization
from .task import Task
from .log import logger


class TQTask(taskqueue.RegisteredTask):
    """
    Wraper around `python-task-queue` task that supports pickle based
    serialization and custom
    """

    def __init__(self, task: Task):
        task_ser = serialization.serialize(task)

        super().__init__(
            task_ser=task_ser,
        )

    def execute(self):
        task = serialization.deserialize(
            self.task_ser  # pylint: disable=no-member # because of tq
        )
        logger.info(f"Starting execution of `{task}`")
        s = time.time()
        task()
        e = time.time()
        logger.info(f"`{task}` done in: {e - s:.3f} seconds.")
