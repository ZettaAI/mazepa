from __future__ import annotations
from mazepa import Job, TaskExecutionEnv


def test_ctx():
    j = Job(
        fn=lambda: None,
        task_execution_env=None,
        id_="job_0",
    )
    env = TaskExecutionEnv()
    with j.task_execution_env_ctx(env):
        assert j.task_execution_env == env


def test_get_batch_env(mocker):
    fn = mocker.MagicMock(
        return_value=iter(
            [
                Job(
                    fn=lambda: None,
                    task_execution_env=None,
                    id_="job_1",
                )
            ]
        )
    )
    j = Job(
        fn=fn,
        task_execution_env=None,
        id_="job_0",
    )
    env = TaskExecutionEnv()
    with j.task_execution_env_ctx(env):
        result = j.get_next_batch()
        assert result[0].task_execution_env == env
