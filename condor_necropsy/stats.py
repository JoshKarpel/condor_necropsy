# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

import math
import datetime
import enum

import pandas as pd

import htcondor

from . import utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SUMMARY_HEADERS = ["Statistic", "Mean", "5%", "25%", "Median", "75%", "95%"]


def chop_microseconds(delta):
    return delta - datetime.timedelta(microseconds=delta.microseconds)


COLUMNS = [
    "note",
    "submitted_at",
    "runtime",
    "memory_usage",
    "time_to_first_start",
    "transfer_output_queue_time",
    "transfer_input_queue_time",
    "transfer_input_time",
    "transfer_output_time",
]


class EventData:
    def __init__(
        self,
        submitted_at,
        note,
        runtime,
        memory_usage,
        time_to_first_start,
        transfer_input_queue_time,
        transfer_output_queue_time,
        transfer_input_time,
        transfer_output_time,
    ):
        self.submitted_at = submitted_at
        self.note = note
        self.runtime = runtime
        self.memory_usage = memory_usage
        self.time_to_first_start = time_to_first_start
        self.transfer_output_queue_time = transfer_output_queue_time
        self.transfer_input_queue_time = transfer_input_queue_time
        self.transfer_input_time = transfer_input_time
        self.transfer_output_time = transfer_output_time

    def as_dataframe(self):
        rows = [
            (key[0], key[1], *(getattr(self, col).get(key, None) for col in COLUMNS))
            for key in sorted(self.submitted_at.keys())
        ]

        df = pd.DataFrame.from_records(
            data=rows, columns=["cluster", "proc"] + COLUMNS, index=["cluster", "proc"]
        )

        for col in df:
            if "time" in col:
                df[col] = df[col].astype("timedelta64[s]")

        return df


def extract_data(events):
    submitted_at = {}
    time_to_first_start = {}
    runtime = {}
    memory_usage = {}
    transfer_input_queued = {}
    transfer_input_queue_time = {}
    transfer_input_start = {}
    transfer_input_time = {}
    transfer_output_queued = {}
    transfer_output_queue_time = {}
    transfer_output_start = {}
    transfer_output_time = {}
    note = {}

    for event in events:
        key = (event.cluster, event.proc)

        if event.type is htcondor.JobEventType.SUBMIT:
            submitted_at[key] = event.timestamp
            note[key] = event.get("LogNotes", None)

        elif event.type is htcondor.JobEventType.EXECUTE:
            if key not in time_to_first_start:
                time_to_first_start[key] = datetime.timedelta(
                    seconds=event.timestamp - submitted_at[key]
                )

        elif event.type is htcondor.JobEventType.IMAGE_SIZE:
            memory_usage[key] = max(
                memory_usage.get(key, 0), int(event["MemoryUsage"]) * (1024 ** 2)
            )

        elif event.type is htcondor.JobEventType.FILE_TRANSFER:
            transfer_event_type = TransferEventType(event["Type"])

            if transfer_event_type is TransferEventType.INPUT_TRANSFER_QUEUED:
                transfer_input_queued[key] = event.timestamp
            elif transfer_event_type is TransferEventType.INPUT_TRANSFER_STARTED:
                transfer_input_start[key] = event.timestamp
                transfer_input_queue_time[key] = datetime.timedelta(
                    seconds=event.timestamp
                    - transfer_input_queued.get(key, event.timestamp)
                )
            elif transfer_event_type is TransferEventType.INPUT_TRANSFER_FINISHED:
                transfer_input_time[key] = datetime.timedelta(
                    seconds=event.timestamp - transfer_input_start[key]
                )
            elif transfer_event_type is TransferEventType.OUTPUT_TRANSFER_QUEUED:
                transfer_output_queued[key] = event.timestamp
            elif transfer_event_type is TransferEventType.OUTPUT_TRANSFER_STARTED:
                transfer_output_start[key] = event.timestamp
                transfer_output_queue_time[key] = datetime.timedelta(
                    seconds=event.timestamp
                    - transfer_output_queued.get(key, event.timestamp)
                )
            elif transfer_event_type is TransferEventType.OUTPUT_TRANSFER_FINISHED:
                try:
                    transfer_output_time[key] = datetime.timedelta(
                        seconds=event.timestamp - transfer_output_start[key]
                    )
                except KeyError:
                    pass

        elif event.type is htcondor.JobEventType.JOB_TERMINATED:
            runtime[key] = parse_runtime(event["RunRemoteUsage"])

    return EventData(
        submitted_at=submitted_at,
        note=note,
        runtime=runtime,
        memory_usage=memory_usage,
        time_to_first_start=time_to_first_start,
        transfer_input_queue_time=transfer_input_queue_time,
        transfer_output_queue_time=transfer_output_queue_time,
        transfer_input_time=transfer_input_time,
        transfer_output_time=transfer_output_time,
    )


def make_summaries(event_data):
    runtime_summary = make_summary(
        event_data.runtime, "Runtime", post_process=chop_microseconds
    )
    time_to_first_start_summary = make_summary(
        event_data.time_to_first_start,
        "Time to First Start",
        post_process=chop_microseconds,
    )
    memory_usage_summary = make_summary(
        event_data.memory_usage, "Memory Usage", post_process=utils.num_bytes_to_str
    )
    transfer_input_summary = make_summary(
        event_data.transfer_input_time,
        "Input Transfer Time",
        post_process=chop_microseconds,
    )
    transfer_output_summary = make_summary(
        event_data.transfer_output_time,
        "Output Transfer Time",
        post_process=chop_microseconds,
    )
    transfer_input_queue_summary = make_summary(
        event_data.transfer_input_queue_time,
        "Input Transfer Queue",
        post_process=chop_microseconds,
    )
    transfer_output_queue_summary = make_summary(
        event_data.transfer_output_queue_time,
        "Output Transfer Queue",
        post_process=chop_microseconds,
    )

    return (
        runtime_summary,
        time_to_first_start_summary,
        transfer_input_summary,
        transfer_output_summary,
        transfer_input_queue_summary,
        transfer_output_queue_summary,
        memory_usage_summary,
    )


class TransferEventType(enum.IntEnum):
    INPUT_TRANSFER_QUEUED = 1
    INPUT_TRANSFER_STARTED = 2
    INPUT_TRANSFER_FINISHED = 3
    OUTPUT_TRANSFER_QUEUED = 4
    OUTPUT_TRANSFER_STARTED = 5
    OUTPUT_TRANSFER_FINISHED = 6


def make_summary(data, name, post_process=None):
    if post_process is None:
        post_process = lambda x: x

    type_ = type(next(iter(data.values())))
    summary = {
        "Mean": sum(data.values(), type_()) / len(data),
        "5%": percentile(data.values(), 0.05),
        "25%": percentile(data.values(), 0.25),
        "Median": percentile(data.values(), 0.5),
        "75%": percentile(data.values(), 0.75),
        "95%": percentile(data.values(), 0.95),
    }

    summary = {k: post_process(v) for k, v in summary.items()}
    summary["Statistic"] = name

    return summary


def parse_runtime(runtime_string: str) -> datetime.timedelta:
    (_, usr_days, usr_hms), (_, sys_days, sys_hms) = [
        s.split() for s in runtime_string.split(",")
    ]

    usr_h, usr_m, usr_s = usr_hms.split(":")
    sys_h, sys_m, sys_s = sys_hms.split(":")

    usr_time = datetime.timedelta(
        days=int(usr_days), hours=int(usr_h), minutes=int(usr_m), seconds=int(usr_s)
    )
    sys_time = datetime.timedelta(
        days=int(sys_days), hours=int(sys_h), minutes=int(sys_m), seconds=int(sys_s)
    )

    return usr_time + sys_time


def percentile(values, percentile, key=None):
    if key is None:
        key = lambda x: x

    values = sorted(values, key=key)

    k = (len(values) - 1) * percentile
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(values[int(k)])
    lower = key(values[int(f)]) * (c - k)
    upper = key(values[int(c)]) * (k - f)
    return lower + upper
