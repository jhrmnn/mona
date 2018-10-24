# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import logging
from typing import List, Optional

import click

from .app import Caf
from .sessions import Session
from .utils import import_fullname

logging.basicConfig(
    style='{',
    format='[{asctime}.{msecs:03.0f}] {levelname}:{name}: {message}',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
logging.getLogger('caf').setLevel(int(os.environ.get('CAF_DEBUG', logging.INFO)))


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    ctx.ensure_object(Caf)


@cli.command()
@click.pass_obj
def init(app: Caf) -> None:
    app.ensure_cafdir()


@cli.command()
@click.option('-p', '--pattern', multiple=True, help='Tasks to be executed')
@click.option('-n', '--jobs', default=1, help='Number of parallel tasks')
@click.option('-l', '--limit', type=int, help='Limit number of tasks to N')
@click.option('--maxerror', default=5, help='Number of errors in row to quit')
@click.argument('rulename', metavar='RULE')
@click.pass_obj
def run(app: Caf,
        pattern: Optional[List[str]],
        jobs: int,
        limit: Optional[int],
        maxerror: int,
        rulename: str) -> None:
    rule = import_fullname(rulename)
    sess = Session()
    app(sess)
    with sess:
        sess.eval(rule())
