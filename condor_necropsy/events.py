from pathlib import Path
import itertools

import htcondor


def get_events(*event_log_paths):
    for path in event_log_paths:
        yield from htcondor.JobEventLog(Path(path).as_posix()).events(0)
