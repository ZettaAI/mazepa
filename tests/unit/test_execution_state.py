from __future__ import annotations

from typing import Union, Any
import pytest
from mazepa import Job, Task, Dependency, InMemoryExecutionState, job, TaskStatus, TaskOutcome


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
def test_job_execution_flow(
    jobs: list[Job],
    max_batch_len: int,
    expected_batch_ids,
    completed_task_ids: list[list[str]],
    expected_ongoing_job_ids
):
    state = InMemoryExecutionState(ongoing_jobs=jobs)

    for i in range(len(expected_batch_ids)):
        batch = state.get_task_batch(max_batch_len=max_batch_len)
        assert [e.id_ for e in batch] == expected_batch_ids[i]

        task_outcomes = {
            id_: TaskOutcome[Any](status=TaskStatus.SUCCEEDED)
            for id_ in completed_task_ids[i]
        }
        state.update_with_task_outcomes(task_outcomes)
        assert state.get_ongoing_job_ids() == expected_ongoing_job_ids[i]


def test_task_outcome_setting():
    # type: () -> None
    task = Task(fn=lambda: None, kwargs={}, id_='a')
    jobs = [
        Job(
            fn=iter,
            args=[[task]],
            id_='job_0',
        )
    ]
    state = InMemoryExecutionState(ongoing_jobs=jobs)
    state.get_task_batch()
    outcomes = {'a': TaskOutcome(status=TaskStatus.SUCCEEDED, return_value=5566)}
    state.update_with_task_outcomes(outcomes)
    assert task.outcome == outcomes['a']

def test_task_outcome_exc():
    # type: () -> None
    task = Task(fn=lambda: None, kwargs={}, id_='a')
    jobs = [
        Job(
            fn=iter,
            args=[[task]],
            id_='job_0',
        )
    ]
    state = InMemoryExecutionState(ongoing_jobs=jobs)
    state.get_task_batch()
    outcomes = {'a': TaskOutcome[Any](status=TaskStatus.FAILED)}
    with pytest.raises(Exception):
        state.update_with_task_outcomes(outcomes)

