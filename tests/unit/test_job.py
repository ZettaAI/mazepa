from __future__ import annotations
from typing import Any
import pytest
from mazepa import Dependency, Job, TaskExecutionEnv

def test_task_env_default():
    j = Job(fn=lambda: None)
    assert j.task_execution_env is None

def test_ctx():
    j = Job(fn=lambda: None)
    env = TaskExecutionEnv()
    with j.task_execution_env_ctx(env):
        assert j.task_execution_env == env


def test_get_batch_env(mocker):
    fn = mocker.MagicMock(return_value=iter([Job(lambda: None)]))
    j = Job(fn=fn)
    env = TaskExecutionEnv()
    with j.task_execution_env_ctx(env):
        result = j.get_next_batch()
        assert result[0].task_execution_env == env
