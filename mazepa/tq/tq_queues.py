from __future__ import annotations
import time
from typing import Iterable, Union, Any, Optional
from collections import defaultdict
import cachetools  # type: ignore
import attrs
import tenacity
import boto3  # type: ignore
from typeguard import typechecked
import taskqueue  # type: ignore

from .. import Task, TaskOutcome
from .. import serialization


@cachetools.cached
def _get_sqs_client(region_name):
    return boto3.client("sqs", reqion_name=region_name)


@cachetools.cached
def _get_queue_url(queue_name: str, region_name) -> str:
    sqs_client = _get_sqs_client(region_name)
    result = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    return result


# @typechecked # makes in not siutable as attrs converter
def _parse_work_queues(queues: Union[str, Iterable[str], dict[str, Any]]) -> dict[str, Any]:
    if isinstance(queues, str):
        queues = [queues]

    if isinstance(queues, dict):
        result = queues
    else:  # iterable of queue names
        result = {name: taskqueue.TaskQueue(name) for name in queues}
    return result


def _send_completion_report(task: Task, queue_name: str, region_name: str):
    completion_queue = taskqueue.TaskQueue(
        queue_name, region_name=region_name, n_threads=0, green=False
    )
    message = [
        {
            "Id": "task_outcome",
            "MessageBody": serialization.serialize({"task_id": task.id_, "outcome": task.outcome}),
        }
    ]
    send_message(completion_queue, message)


def _receive_messages(
    queue_name: str, region_name: str, max_message_num: int = 100, max_time_sec: float = 2.0
) -> list[dict]:
    result = []  # type: list[dict]
    start_ts = time.time()
    while True:
        resp = _get_sqs_client(region_name).receive_message(
            QueueUrl=_get_queue_url(queue_name, region_name),
            AttributeNames=["All"],
            MaxNumberOfMessages=10,
        )

        if "Messages" not in resp:
            break

        entries = []

        # iteration over i to preserve the "Id"
        for i in range(len(resp["Messages"])):
            entries.append({"ReceiptHandle": resp["Messages"][i]["ReceiptHandle"], "Id": str(i)})
            result.append(serialization.deserialize(resp["Messages"][i]["Body"]))

        _get_sqs_client(region_name).delete_message_batch(
            QueueUrl=_get_queue_url(queue_name, region_name), Entries=entries
        )

        if len(result) >= max_message_num:
            break
        now_ts = time.time()
        if now_ts - start_ts >= max_time_sec:
            break

    return result


@tenacity.retry(stop=tenacity.stop_after_attempt(30), wait=tenacity.wait_random(min=1, max=2))
def send_message(queue, message):
    msg_ack = queue.api.sqs.send_message_batch(QueueUrl=queue.api.qurl, Entries=message)
    if "Successful" not in msg_ack:
        raise RuntimeError(f"Unable to send message {message}.")


@typechecked
@attrs.mutable
class TQExecutionMultiqueue:
    queues: dict[str, Any] = attrs.field(converter=_parse_work_queues)
    insertion_threads: int = 0
    completion_queue_name: Optional[str] = None
    completion_region_name: str = attrs.field(default=taskqueue.secrets.AWS_DEFAULT_REGION)

    def purge(self):
        raise NotImplementedError()

    def _add_completion_callbacks(self, tasks: Iterable[Task]):
        if self.completion_queue_name is None:
            raise RuntimeError(
                "Completion queue name not specified."
            )

        for task in tasks:
            callbacks.append(
                ComparablePartial
            )

    def push_tasks(self, tasks: Iterable[Task]):
        tasks_for_queue = defaultdict(list)
        for task in tasks:
            matching_queue_names = [
                name
                for name in self.queues.keys()
                if all(tag in name for tag in task.task_execution_env.queue_tags)
            ]
            if len(matching_queue_names) == 0:
                raise RuntimeError(
                    f"No queue from set {list(self.queues.keys())} matches "
                    f"all tags {task.task_execution_env.queue_tags}."
                )
            tasks_for_queue[matching_queue_names[0]].append(task)

        for k, v in tasks_for_queue.items():
            self.queues[k].insert(v, parallel=self.insertion_threads)

    def pull_task_outcomes(self) -> dict[str, TaskOutcome]:
        if self.completion_queue_name is None:
            raise RuntimeError(
                "Attempting to pull task oucomes without completion queue beign specified"
            )

        messages = _receive_messages(
            queue_name=self.completion_queue_name,
            region_name=self.completion_region_name,
        )

        result = {
            e['id']: e['outcome'] for e in messages
        }
        return result
