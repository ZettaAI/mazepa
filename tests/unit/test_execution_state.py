from __future__ import annotations

from typing import Any, Union
import pytest
from mazepa import Job, Task, Dependency, InMemoryExecutionState, job

@job
def dummy_job(yields: list):
    for e in yields:
        yield e

@pytest.mark.parametrize(
    'job_specs, max_batch_len, expected_batch_ids, completed_task_ids, expected_ongoing_job_ids',
    [



    ]
)
def adsf_test_job_execution(
    job_specs: dict[str, list[Union[Task, Job, Dependency]]],
    max_batch_len: int,
    expected_batch_ids,
    completed_task_ids,
    expected_ongoing_job_ids
):
    jobs = {
        k: dummy_job(v) for k, v in job_specs.items()
    }
    for k, v in jobs.items():
        v.id_ = k

    state = InMemoryExecutionState(jobs)
    assert state.get_ongoing_job_ids() == list(jobs.keys())

    for i in range(len(expected_batch_ids)):
        batch = state.get_task_batch(max_batch_len=max_batch_len)
        assert [e.id_ for e in batch] == expected_batch_ids[i]

        state.update_completed_task_ids(completed_task_ids[i])
        assert state.get_ongoing_job_ids() == expected_ongoing_job_ids[i]


@pytest.mark.parametrize(
    'jobs, max_batch_len, expected_batch_ids, completed_task_ids, expected_ongoing_job_ids',
    [
        [
            [
                Job(
                    fn=iter,
                    args=[[Task(fn=lambda: None, kwargs={}, id_=id_) for id_ in ['a', 'b', 'c']]],
                    id_='job_0',
                )
            ],
            10,
            [['a', 'b', 'c']],
            [[]],
            [['job_0']]
        ],
        [
            [
                Job(
                    fn=iter,
                    args=[
                       [Task(fn=lambda: None, kwargs={}, id_=id_) for id_ in ['a', 'b', 'c']]
                    ],
                    id_='job_0',
                )
            ],
            10,
            [['a', 'b', 'c']],
            [['a', 'b']],
            [['job_0']]
        ],
        [
            [
                Job(
                    fn=iter,
                    args=[
                       [Task(fn=lambda: None, kwargs={}, id_=id_) for id_ in ['a', 'b', 'c']]
                    ],
                    id_='job_0',
                )
            ],
            10,
            [['a', 'b', 'c']],
            [['a', 'b', 'c']],
            [[]]
        ],
        [
            [
                Job(
                    fn=iter,
                    args=[
                       [Task(fn=lambda: None, kwargs={}, id_=id_) for id_ in ['a', 'b', 'c']]
                    ],
                    id_='job_0',
                )
            ],
            1,
            [['a']],
            [['a']],
            [['job_0']],
        ],
        [
            [
                Job(
                    fn=iter,
                    args=[
                       [
                           Task(fn=lambda: None, kwargs={}, id_='a'),
                           Dependency(),
                           Task(fn=lambda: None, kwargs={}, id_='b'),
                       ]
                    ],
                    id_='job_0',
                )
            ],
            10,
            [['a'], ['b']],
            [['a'], ['b']],
            [['job_0'], []],
        ],
        [
            [
                Job(
                    fn=iter,
                    args=[
                       [
                           Task(fn=lambda: None, kwargs={}, id_='a'),
                           Dependency(),
                           Task(fn=lambda: None, kwargs={}, id_='b'),
                       ]
                    ],
                    id_='job_0',
                )
            ],
            10,
            [['a'], ['b'], []],
            [['a'], [], ['b']],
            [['job_0'], ['job_0'], []],
        ],
        [
            [
                Job(
                    fn=iter,
                    args=[[
                       [
                           Task(fn=lambda: None, kwargs={}, id_='a'),
                           Task(fn=lambda: None, kwargs={}, id_='b'),
                           Task(fn=lambda: None, kwargs={}, id_='c'),
                       ],
                       Dependency(['a']),
                       [
                           Task(fn=lambda: None, kwargs={}, id_='d'),
                       ],
                    ]],
                    id_='job_0',
                )
            ],
            10,
            [['a', 'b', 'c'], [], ['d']],
            [['b'], ['a'], ['c', 'd']],
            [['job_0'], ['job_0'], []],
        ],
        [
            [
                Job(
                    fn=iter,
                    args=[[
                       [
                           Task(fn=lambda: None, kwargs={}, id_='a'),
                           Task(fn=lambda: None, kwargs={}, id_='b'),
                           Task(fn=lambda: None, kwargs={}, id_='c'),
                       ],
                       Dependency(['a']),
                       [
                           Task(fn=lambda: None, kwargs={}, id_='d'),
                       ],
                    ]],
                    id_='job_0',
                )
            ],
            10,
            [['a', 'b', 'c'], [], ['d']],
            [['b'], ['a'], ['c', 'd']],
            [['job_0'], ['job_0'], []],
        ],
        [
            [
               Job(
                   fn = iter,
                   args=[[
                       Task(fn=lambda: None, kwargs={}, id_='a'),
                       Dependency(),
                       Task(fn=lambda: None, kwargs={}, id_='b'),
                   ]],
                   id_='job_0'
               )
            ],
            10,
            [['a'], ['b'], []],
            [['a'], [], ['b']],
            [['job_0'], ['job_0'], []],
        ],
        [
            [
               Job(
                   fn = iter,
                   args=[[
                       Job(
                           fn = iter,
                           args=[[
                               Task(fn=lambda: None, kwargs={}, id_='x'),
                               Task(fn=lambda: None, kwargs={}, id_='y'),
                               Dependency(['x']),
                               Task(fn=lambda: None, kwargs={}, id_='z'),
                           ]],
                           id_='job_1'
                       ),
                       Task(fn=lambda: None, kwargs={}, id_='a'),
                       Dependency('a'),
                       Task(fn=lambda: None, kwargs={}, id_='b'),
                   ]],
                   id_='job_0'
               )
            ],
            10,
            [['a'], ['x', 'y'], ['b'], ['z']],
            [[], ['y', 'a'], ['x'], ['z']],
            [['job_0', 'job_1'], ['job_0', 'job_1'], ['job_0', 'job_1'], ['job_0']],
        ],
    ]
)
def test_job_execution_v2(
    jobs: list[Job],
    max_batch_len: int,
    expected_batch_ids,
    completed_task_ids,
    expected_ongoing_job_ids
):
    state = InMemoryExecutionState(ongoing_jobs=jobs)

    for i in range(len(expected_batch_ids)):
        batch = state.get_task_batch(max_batch_len=max_batch_len)
        assert [e.id_ for e in batch] == expected_batch_ids[i]

        state.update_completed_task_ids(completed_task_ids[i])
        assert state.get_ongoing_job_ids() == expected_ongoing_job_ids[i]

