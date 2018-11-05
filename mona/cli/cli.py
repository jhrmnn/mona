# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Any, cast, Dict, Tuple, Sequence

import click

from ..tasks import Task
from ..futures import STATE_COLORS
from ..utils import import_fullname, groupby
from ..files import File
from ..rules.dirtask import checkout_files, DirtaskInput
from .glob import match_glob
from .app import App
from .table import Table, lenstr

__version__ = '0.1.0'

logging.basicConfig(
    style='{',
    format='[{asctime}.{msecs:03.0f}] {levelname}:{name}: {message}',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
logging.getLogger('mona').setLevel(int(os.environ.get('MONA_DEBUG', logging.INFO)))


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    ctx.ensure_object(App)


@cli.command()
@click.pass_obj
def init(app: App) -> None:
    """Initialize a Git repository."""
    app.ensure_monadir()


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
        if self._maxerror is None:
            return False
        if self._n_errors == self._maxerror:
            log.warn('Maximum number of errors reached')
        self._n_errors += 1
        if self._n_errors <= self._maxerror:
            return True
        return False


@cli.command()
@click.option('-p', '--pattern', multiple=True, help='Tasks to be executed')
@click.option('-P', '--path', is_flag=True, help='Execute path-like tasks')
@click.option('-j', '--cores', type=int, help='Number of cores')
@click.option('-l', '--limit', type=int, help='Limit number of tasks to N')
@click.option('--maxerror', type=int, help='Number of errors in row to quit')
@click.argument('rulename', metavar='RULE', envvar='MONA_RULE')
@click.pass_obj
def run(
    app: App,
    pattern: List[str],
    cores: int,
    path: bool,
    limit: Optional[int],
    maxerror: int,
    rulename: str,
) -> None:
    """Run a given rule."""
    rule = import_fullname(rulename)
    task_filter = TaskFilter(pattern, no_path=not path)
    exception_buffer = ExceptionBuffer(maxerror)
    with app.session(ncores=cores) as sess:
        sess.eval(
            rule(),
            exception_handler=exception_buffer,
            task_filter=task_filter,
            limit=limit,
        )


@cli.command()
@click.option('-p', '--pattern', multiple=True, help='Patterns to be reported')
@click.pass_obj
def status(app: App, pattern: List[str]) -> None:
    """Print status of tasks."""
    sess = app.session(warn=False, readonly=True, full_restore=True)
    ncols = len(STATE_COLORS) + 1
    table = Table(align=['<', *(ncols * ['>'])], sep=['   ', *((ncols - 1) * ['/'])])
    table.add_row('pattern', *(s.name.lower() for s in STATE_COLORS), 'all')
    with sess:
        app.cache.restore_last()
        task_groups: Dict[str, List[Task[object]]] = {}
        all_tasks = list(sess.all_tasks())
    for patt in pattern or ['**']:
        matched_any = False
        for task in all_tasks:
            matched = match_glob(task.label, patt)
            if matched:
                task_groups.setdefault(matched, []).append(task)
                matched_any = True
        if not matched_any:
            task_groups[patt] = []
    for label, tasks in task_groups.items():
        grouped = {
            state: group
            for state, group in groupby(tasks, key=lambda t: t.state).items()
        }
        counts: List[Tuple[int, Optional[str]]] = [
            (len(grouped.get(state, [])), color)
            for state, color in STATE_COLORS.items()
        ]
        counts.append((len(tasks), None))
        col_counts = [
            lenstr(click.style(str(count), fg=color), len(str(count)))
            for count, color in counts
        ]
        table.add_row(label, *col_counts)
    click.echo(str(table))


@cli.command()
@click.pass_obj
def graph(app: App) -> None:
    """Open a pdf with the task graph."""
    sess = app.session(warn=False, readonly=True, full_restore=True)
    with sess:
        app.cache.restore_last()
        dot = sess.dot_graph()
    dot.render(tempfile.mkstemp()[1], view=True, cleanup=True, format='pdf')


@cli.command()
@click.option('-p', '--pattern', multiple=True, help='Tasks to be checked out')
@click.option('--done', is_flag=True, help='Check out only finished tasks')
@click.option('-c', '--copy', is_flag=True, help='Copy instead of symlinking')
@click.pass_obj
def checkout(app: App, pattern: List[str], done: bool, copy: bool) -> None:
    """Checkout path-labeled tasks into a directory tree."""
    n_tasks = 0
    sess = app.session(warn=False, readonly=True, full_restore=True)
    with sess:
        app.cache.restore_last()
        for task in sess.all_tasks():
            if task.label[0] != '/':
                continue
            if pattern and not any(match_glob(task.label, patt) for patt in pattern):
                continue
            if done and not task.done():
                continue
            exe: Optional[File]
            paths: Sequence[DirtaskInput]
            if task.rule == 'dir_task':
                exe = cast(File, task.args[0].value)
                paths = cast(List[DirtaskInput], task.args[1].value)
                if task.done():
                    paths.extend(cast(Dict[str, File], task.result()).values())
            elif task.rule == 'file_collection':
                exe = None
                paths = cast(List[File], task.args[0].value)
            root = Path(task.label[1:])
            root.mkdir(parents=True, exist_ok=True)
            checkout_files(root, exe, paths, mutable=copy)
            n_tasks += 1
    log.info(f'Checked out {n_tasks} tasks.')
