# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import logging
from typing import List, Optional, Any

import click

from ..tasks import Task
from ..utils import import_fullname
from .glob import match_glob
from .app import App

logging.basicConfig(
    style='{',
    format='[{asctime}.{msecs:03.0f}] {levelname}:{name}: {message}',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
logging.getLogger('caf').setLevel(int(os.environ.get('CAF_DEBUG', logging.INFO)))


@click.group(chain=True)
@click.pass_context
def cli(ctx: click.Context) -> None:
    ctx.ensure_object(App)


@cli.command()
@click.pass_obj
def init(app: App) -> None:
    app.ensure_cafdir()


class TaskFilter:
    def __init__(self, patterns: List[str] = None, no_path: bool = False) -> None:
        self._patterns = patterns or []
        self._no_path = no_path

    def __call__(self, task: Task[Any]) -> bool:
        if self._no_path and task.label.startswith('/'):
            return False
        if self._patterns and not any(
            match_glob(task.label, patt) for patt in self._patterns
        ):
            return False
        return True


class ExceptionBuffer:
    def __init__(self, maxerror: int = None) -> None:
        self._maxerror = maxerror
        self._n_errors = 0

    def __call__(self, task: Task[Any], exc: Exception) -> bool:
        if self._maxerror is not None:
            assert self._n_errors <= self._maxerror
            if self._n_errors == self._maxerror:
                log.warn('Maximum number of errors reached')
                return False
        return True


@cli.command()
@click.argument('rulename', metavar='RULE')
@click.pass_obj
def conf(app: App, rulename: str) -> None:
    rule = import_fullname(rulename)
    task_filter = TaskFilter(no_path=True)
    with app.session(full_restore=True) as sess:
        sess.eval(rule(), task_filter=task_filter)


@cli.command()
@click.option('-p', '--pattern', multiple=True, help='Tasks to be executed')
@click.option('-j', '--cores', type=int, help='Number of cores')
@click.option('-l', '--limit', type=int, help='Limit number of tasks to N')
@click.option('--maxerror', default=5, help='Number of errors in row to quit')
@click.argument('rulename', metavar='RULE')
@click.pass_obj
def run(
    app: App,
    pattern: List[str],
    cores: int,
    limit: Optional[int],
    maxerror: int,
    rulename: str,
) -> None:
    rule = import_fullname(rulename)
    task_filter = TaskFilter(pattern)
    exception_buffer = ExceptionBuffer(maxerror)
    with app.session(ncores=cores) as sess:
        sess.eval(
            rule(),
            exception_handler=exception_buffer,
            task_filter=task_filter,
            limit=limit,
        )
