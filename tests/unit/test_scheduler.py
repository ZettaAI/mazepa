import pytest

import mazepa


def test_scheduler_simple(mocker):
    queue_mock = mocker.MagicMock()
    task_mocks = [mocker.MagicMock() for i in range(3)]
    job1_mock = mocker.MagicMock(side_effect=task_mocks)

    s = mazepa.Scheduler(queue=queue_mock)
    s.add_job(job1_mock)
    s.execute()

    queue_mock.get_next_batch.assert_called()
    for t in task_mocks:
        t.assert_called()

def test_scheduler_max_batch_size(mocker):
    pass

def test_scheduler_multijob(mocker):
    pass

def test_scheduler_dependency(mocker):
    pass

def test_scheduler_list_yield(mocker):
    pass

def test_scheduler_job_yield(mocker):
    pass

def test_scheduler_resume(mocker):
    pass
