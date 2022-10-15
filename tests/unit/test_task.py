from __future__ import annotations
from mazepa import task_maker_cls, Task


class DummyTaskMaker:
    def __call__(self):
        pass


def test_make_task_maker_cls():
    cls_wrapped = task_maker_cls(DummyTaskMaker)
    task_maker = cls_wrapped()
    task = task_maker.make_task()  # pylint: disable=no-member
    assert isinstance(task, Task)
