from __future__ import annotations
from mazepa import task_factory, Task, task_factory_cls, TaskFactory


def dummy_task_fn():
    pass


class DummyTaskCls:
    def __call__(self):
        return "result"


def test_make_task_factory_cls() -> None:
    cls = task_factory_cls(DummyTaskCls)
    obj = cls()
    assert isinstance(obj, TaskFactory)
    task = obj.make_task()
    assert isinstance(task, Task)
    outcome = obj()
    assert outcome == "result"


def test_make_task_factory() -> None:
    fn = task_factory(dummy_task_fn)
    assert isinstance(fn, TaskFactory)
    task = fn.make_task()
    assert isinstance(task, Task)
