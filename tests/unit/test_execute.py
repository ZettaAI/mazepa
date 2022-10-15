# pylint: disable=global-statement,redefined-outer-name,unused-argument
from __future__ import annotations
from typing import Any
import pytest
from mazepa import (
    Dependency,
    job,
    TaskStatus,
    task_maker,
    execute,
    InMemoryExecutionState,
    LocalExecutionQueue,
)
from mazepa.remote_execution_queues import SQSExecutionQueue

TASK_COUNT = 0


@pytest.fixture
def reset_task_count():
    global TASK_COUNT
    TASK_COUNT = 0


@task_maker
def dummy_task(return_value: Any) -> Any:
    global TASK_COUNT
    TASK_COUNT += 1
    return return_value


@job
def dummy_job():
    task1 = dummy_task.make_task(return_value="output1")
    yield task1
    yield Dependency([task1.id_])
    assert task1.outcome.status == TaskStatus.SUCCEEDED
    assert task1.outcome.return_value == "output1"
    task2 = dummy_task.make_task(return_value="output2")
    yield task2


@job
def empty_job():
    yield []


def test_local_execution_defaults(reset_task_count):
    execute(
        [
            dummy_job(),
            dummy_job(),
            dummy_job(),
        ],
        batch_gap_sleep_sec=0,
        max_batch_len=2,
        purge_at_start=True,
    )
    assert TASK_COUNT == 6


def test_local_execution_one_job(reset_task_count):
    execute(
        dummy_job(),
        batch_gap_sleep_sec=0,
        max_batch_len=1,
    )
    assert TASK_COUNT == 2


def test_local_execution_state(reset_task_count):
    execute(
        InMemoryExecutionState(
            [
                dummy_job(),
                dummy_job(),
                dummy_job(),
            ]
        ),
        batch_gap_sleep_sec=0,
        max_batch_len=2,
        purge_at_start=True,
    )
    assert TASK_COUNT == 6


def test_local_execution_state_queue(reset_task_count):
    execute(
        InMemoryExecutionState(
            [
                dummy_job(),
                dummy_job(),
                dummy_job(),
            ]
        ),
        exec_queue=LocalExecutionQueue(),
        batch_gap_sleep_sec=0,
        max_batch_len=2,
        purge_at_start=True,
    )
    assert TASK_COUNT == 6


def test_local_no_sleep(mocker):
    sleep_m = mocker.patch("time.sleep")
    execute(
        empty_job(),
        batch_gap_sleep_sec=10,
        max_batch_len=2,
        purge_at_start=True,
    )
    sleep_m.assert_not_called()


def test_non_local_sleep(mocker):
    sleep_m = mocker.patch("time.sleep")
    queue_m = mocker.MagicMock(spec=SQSExecutionQueue)
    execute(
        empty_job(),
        batch_gap_sleep_sec=10,
        max_batch_len=2,
        purge_at_start=True,
        exec_queue=queue_m,
    )
    sleep_m.assert_called_once()
