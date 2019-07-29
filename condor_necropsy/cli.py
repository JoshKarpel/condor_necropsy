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

import click
from click_didyoumean import DYMGroup

from halo import Halo
from spinners import Spinners

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SPINNERS = list(name for name in Spinners.__members__ if name.startswith('dots'))


def make_spinner(*args, **kwargs):
    return Halo(
        *args,
        spinner = random.choice(SPINNERS),
        stream = sys.stderr,
        **kwargs,
    )


CONTEXT_SETTINGS = dict(help_option_names = ['-h', '--help'])


@click.group(context_settings = CONTEXT_SETTINGS, cls = DYMGroup)
@click.option(
    '--verbose', '-v',
    is_flag = True,
    default = False,
    help = 'Show log messages as the CLI runs.',
)
def cli(verbose):
    """condor_necropsy command line tool."""
    logger.debug(f'CLI called with arguments "{" ".join(sys.argv[1:])}"')
    if verbose:
        _start_logger()


def _start_logger():
    """Initialize a basic logger for condor_necropsy for the CLI."""
    htmap_logger = logging.getLogger('condor_necropsy')
    htmap_logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream = sys.stderr)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s ~ %(levelname)s ~ %(name)s:%(funcName)s:%(lineno)d ~ %(message)s'))

    htmap_logger.addHandler(handler)

    return handler


if __name__ == '__main__':
    cli()
