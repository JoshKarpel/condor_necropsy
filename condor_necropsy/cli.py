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
import sys
import random
import functools

import click
from click_didyoumean import DYMGroup

from halo import Halo
from spinners import Spinners

from .events import get_events
from .state_graph import make_state_graph
from .stats import get_timing_stats_summaries, SUMMARY_HEADERS
from .table import table
from .version import version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SPINNERS = list(name for name in Spinners.__members__ if name.startswith("dots"))


def make_spinner(*args, **kwargs):
    return Halo(*args, spinner=random.choice(SPINNERS), stream=sys.stderr, **kwargs)


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS, cls=DYMGroup)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show log messages as the CLI runs.",
)
def cli(verbose):
    """condor_necropsy command line tool."""
    logger.debug(f'CLI called with arguments "{" ".join(sys.argv[1:])}"')
    if verbose:
        _start_logger()


def _start_logger():
    """Initialize a basic logger for condor_necropsy for the CLI."""
    logger = logging.getLogger("condor_necropsy")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s ~ %(levelname)s ~ %(name)s:%(funcName)s:%(lineno)d ~ %(message)s"
        )
    )

    logger.addHandler(handler)

    return handler


@cli.command()
def version():
    """Print condor_necropsy version information."""
    click.echo(version())


@cli.command()
@click.argument("logs", nargs=-1, type=click.Path(exists=True, resolve_path=True))
def graph(logs):
    """Make a graph showing the status of the jobs in the logs over time."""
    with make_spinner("Processing events...") as spinner:
        graph = make_state_graph(get_events(*logs))

    click.echo(graph)


_HEADER_FMT = functools.partial(click.style, bold=True)


@cli.command()
@click.argument("logs", nargs=-1, type=click.Path(exists=True, resolve_path=True))
def stats(logs):
    """Display summary statistics for a variety of metrics, like runtime and memory usage."""
    with make_spinner("Processing events...") as spinner:
        stats = get_timing_stats_summaries(get_events(*logs))

    tab = table(headers=SUMMARY_HEADERS, rows=stats, header_fmt=_HEADER_FMT)

    click.echo(tab)


if __name__ == "__main__":
    cli()
