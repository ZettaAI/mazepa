from __future__ import annotations
from mazepa import Flow, TaskExecutionEnv, FlowType, flow_type, flow_type_cls
from mazepa.flows import _Flow, _FlowType


def dummy_flow_fn():
    yield []


class DummyFlowCls:
    def __call__(self):
        yield []


def test_make_flow_type_cls():
    cls = flow_type_cls(DummyFlowCls)
    obj = cls()
    assert isinstance(obj, FlowType)
    flow = obj()
    assert isinstance(flow, Flow)


def test_make_flow_type():
    fn = flow_type(dummy_flow_fn)
    assert isinstance(fn, FlowType)
    flow = fn()
    assert isinstance(flow, Flow)


def test_ctx():
    j = _Flow(
        fn=lambda: None,
        task_execution_env=None,
        id_="flow_0",
    )
    env = TaskExecutionEnv()
    with j.task_execution_env_ctx(env):
        assert j.task_execution_env == env


def test_get_batch_env(mocker):
    fn = mocker.MagicMock(
        return_value=iter(
            [
                _FlowType(
                    fn=lambda: None,
                )()
            ]
        )
    )
    j = _FlowType(
        fn=fn,
    )()
    env = TaskExecutionEnv()
    with j.task_execution_env_ctx(env):
        result = j.get_next_batch()
        assert result[0].task_execution_env == env
