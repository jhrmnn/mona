# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, cast

import click

from .app import Mona
from .dirtask import DirtaskInput, checkout_files
from .files import File
from .futures import STATE_COLORS, State
from .table import Table, lenstr
from .tasks import Task
from .utils import groupby, import_fullname, match_glob

__version__ = '0.1.0'
__all__ = ()

log = logging.getLogger(__name__)


class NaturalOrderGroup(click.Group):
    def list_commands(self, ctx):  # type: ignore
        return self.commands.keys()


@click.group(cls=NaturalOrderGroup)
@click.option('--app', 'appname', envvar='MONA_APP', required=True)
@click.option('--debug', is_flag=True, envvar='MONA_DEBUG')
@click.pass_context
def cli(ctx: click.Context, appname: str, debug: int) -> None:
    package = Path(appname.split(':')[0].split('.')[0])
    if package.is_dir() or package.with_suffix('.py').is_file():
        sys.path.insert(0, '')
    ctx.obj = import_fullname(appname)
    assert isinstance(ctx.obj, Mona)
    if debug:
        log_format = '[{asctime}.{msecs:03.0f}] {levelname}:{name}: {message}'
        log_level = logging.DEBUG
    else:
        log_format = '{message}'
        log_level = logging.INFO
    logging.basicConfig(style='{', format=log_format, datefmt='%H:%M:%S')
    logging.getLogger('mona').setLevel(log_level)


@cli.command()
@click.pass_obj
def init(app: Mona) -> None:
    """Initialize a Git repository."""
    app.ensure_initialized()


class TaskFilter:
    def __init__(self, patterns: List[str] = None, no_path: bool = False) -> None:
        self._patterns = patterns or []
        self._no_path = no_path

    def __call__(self, task: Task[object]) -> bool:
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

    def __call__(self, task: Task[object], exc: Exception) -> bool:
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
@click.argument('entry')
@click.argument('args', nargs=-1)
@click.pass_obj
def run(
    app: Mona,
    pattern: List[str],
    cores: Optional[int],
    path: bool,
    limit: Optional[int],
    maxerror: Optional[int],
    entry: str,
    args: List[str],
) -> None:
    """Run a given rule."""
    app.last_entry = entry_args = [entry, *args]
    task_filter = TaskFilter(pattern, no_path=not path)
    exception_buffer = ExceptionBuffer(maxerror)
    with app.create_session(ncores=cores) as sess:
        result = sess.eval(
            app.call_entry(*entry_args),
            exception_handler=exception_buffer,
            task_filter=task_filter,
            limit=limit,
        )
    if app.get_entry(entry).stdout:
        log.info('Printing result to standard output.')
        print(result)


@cli.command(context_settings={'ignore_unknown_options': True})
@click.argument('profile')
@click.option('-j', '--jobs', type=int, default=1, help='Number of launched workers')
@click.option(
    'env_vars',
    '-v',
    '--var',
    multiple=True,
    help='Environment variable for worker profile',
)
@click.argument('args', nargs=-1)
@click.pass_obj
def dispatch(
    app: Mona, profile: str, args: List[str], jobs: int, env_vars: List[str]
) -> None:
    """Dispatch a Mona command to external workers."""
    worker = Path(f'~/.config/mona/worker_{profile}').expanduser()
    cmd = [str(worker), *args]
    for _ in range(jobs):
        try:
            subprocess.run(
                cmd,
                check=True,
                env={
                    **os.environ,
                    **dict(cast(Tuple[str, str], x.split('=', 1)) for x in env_vars),
                },
            )
        except subprocess.CalledProcessError:
            log.error(f'Running {worker} failed.')


@cli.command()
@click.option('-p', '--pattern', multiple=True, help='Patterns to be reported')
@click.pass_obj
def status(app: Mona, pattern: List[str]) -> None:
    """Print status of tasks."""
    ncols = len(STATE_COLORS) + 1
    table = Table(align=['<', *(ncols * ['>'])], sep=['   ', *((ncols - 1) * ['/'])])
    table.add_row('pattern', *(s.name.lower() for s in STATE_COLORS), 'all')
    with app.create_session(warn=False, write='never', full_restore=True) as sess:
        app.call_last_entry()
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


@cli.group('list')
def list_() -> None:
    """List various objects."""
    pass


@list_.command('tasks')
@click.option(
    'patterns', '-p', '--pattern', multiple=True, help='Patterns to be listed'
)
@click.option('do_finished', '--finished', is_flag=True, help='List finished tasks')
@click.option(
    'do_unfinished', '--unfinished', is_flag=True, help='List unfinished tasks'
)
@click.option('do_running', '--running', is_flag=True, help='List running tasks')
@click.option('do_error', '--error', is_flag=True, help='List tasks in error')
@click.option('disp_hash', '--hash', is_flag=True, help='Display task hash')
@click.option('disp_label', '--label', is_flag=True, help='Display task label')
# @click.option('disp_tmp', '--tmp', is_flag=True, help='Display temporary directory')
@click.option('--no-color', is_flag=True, help='Do not color paths')
@click.pass_obj
def list_tasks(
    app: Mona,
    patterns: List[str],
    do_finished: bool,
    do_unfinished: bool,
    do_running: bool,
    do_error: bool,
    disp_hash: bool,
    disp_label: bool,
    # disp_tmp: bool,  TODO
    no_color: bool,
) -> None:
    """List tasks."""
    with app.create_session(warn=False, write='never', full_restore=True) as sess:
        app.call_last_entry()
        all_tasks = list(sess.all_tasks())
    for task in all_tasks:
        if do_finished and task.state is not State.DONE:
            continue
        if do_error and task.state is not State.ERROR:
            continue
        if do_unfinished and task.state is State.DONE:
            continue
        if do_running and task.state is not State.RUNNING:
            continue
        label = task.label
        if not no_color:
            label = click.style(task.label, fg=STATE_COLORS[task.state])
        if disp_hash:
            line: str = task.hashid
        # elif tmp:
        #     if queue[hashid][2]:
        #         line = queue[hashid][2]
        #     else:
        #         continue
        elif disp_label:
            line = label
        else:
            line = f'{task.hashid} {label}'
        sys.stdout.write(line + '\n')


@cli.command()
@click.option('-p', '--pattern', multiple=True, help='Tasks to be reset')
@click.option('--running', is_flag=True, help='Also reset running tasks')
@click.option('--only-running', is_flag=True, help='Only reset running tasks')
# TODO
# @click.option(
#     '--hard', is_flag=True, help='Also reset finished tasks and remove outputs'
# )
@click.pass_obj
def reset(
    app: Mona, pattern: List[str], running: bool, only_running: bool, hard: bool = False
) -> None:
    """Remove all temporary checkouts and set tasks to clean."""
    if hard and input('Are you sure? ["y" to confirm] ') != 'y':
        return
    states_to_reset = set()
    if not only_running:
        states_to_reset.add(State.ERROR)
    if only_running or running or hard:
        states_to_reset.add(State.RUNNING)
    with app.create_session(warn=False, write='on_exit', full_restore=True) as sess:
        app.call_last_entry()
        for task in sess.all_tasks():
            if pattern and not any(match_glob(task.label, patt) for patt in pattern):
                continue
            if task.state in states_to_reset:
                task.set_state(State.READY)


@cli.command()
@click.argument('file', type=Path, required=False)
@click.pass_obj
def graph(app: Mona, file: Optional[Path]) -> None:
    """Create or open a pdf with the task graph."""
    with app.create_session(warn=False, write='never', full_restore=True) as sess:
        app.call_last_entry()
        dot = sess.dot_graph()
    fmt = file.suffix[1:] if file else 'pdf'
    tgt = dot.render(tempfile.mkstemp()[1], cleanup=True, format=fmt, view=not file)
    if file:
        shutil.move(tgt, file)


@cli.command()
@click.option('-p', '--pattern', multiple=True, help='Tasks to be checked out')
@click.option('--done', is_flag=True, help='Check out only finished tasks')
@click.option('-c', '--copy', is_flag=True, help='Copy instead of symlinking')
@click.pass_obj
def checkout(app: Mona, pattern: List[str], done: bool, copy: bool) -> None:
    """Checkout path-labeled tasks into a directory tree."""
    n_tasks = 0
    with app.create_session(warn=False, write='never', full_restore=True) as sess:
        app.call_last_entry()
        for task in sess.all_tasks():
            if task.label[0] != '/':
                continue
            if pattern and not any(match_glob(task.label, patt) for patt in pattern):
                continue
            if done and not task.done():
                continue
            exe: Optional[File] = None
            paths: Iterable[DirtaskInput]
            if task._func.__name__ == 'dir_task':
                exe = cast(File, task.args[0].value)
                paths = cast(List[DirtaskInput], task.args[1].value)
                if task.done():
                    paths.extend(cast(Dict[str, File], task.result()).values())
            elif task._func.__name__ == 'file_collection':
                paths = cast(List[File], task.args[0].value)
            else:
                if task.done():
                    paths = cast(Dict[str, File], task.result()).values()
            root = Path(task.label[1:])
            root.mkdir(parents=True, exist_ok=True)
            checkout_files(root, exe, paths, mutable=copy)
            n_tasks += 1
    log.info(f'Checked out {n_tasks} tasks.')


@cli.group()
def remote() -> None:
    """Manage remote repositories."""
    pass


@remote.command('add')
@click.argument('name')
@click.argument('url')
@click.pass_obj
def remote_add(app: Mona, url: str, name: str) -> None:
    """Add a remote."""
    host, path = url.split(':')
    name = name or host
    with app.update_config() as config:
        config.setdefault('remotes', {})[name] = {'host': host, 'path': path}


@cli.command()
@click.option('--delete', is_flag=True, help='Delete files when syncing')
@click.option('--dry', is_flag=True, help='Do a dry run')
@click.argument('remotes')
@click.pass_obj
def update(app: Mona, remotes: str, delete: bool, dry: bool) -> None:
    """Update remotes."""
    for remote in app.parse_remotes(remotes):
        remote.update(delete=delete, dry=dry)


@cli.command()
@click.argument('shellcmd')
def cmd(shellcmd: str) -> None:
    """Execute a shell command."""
    subprocess.run(shellcmd, shell=True, check=True)


@cli.command()
@click.argument('remotes')
@click.pass_obj
def go(app: Mona, remotes: str) -> None:
    """SSH into the remote repository."""
    for remote in app.parse_remotes(remotes):
        remote.go()


@cli.command(context_settings={'ignore_unknown_options': True})
@click.argument('remotes')
@click.argument('args', nargs=-1)
@click.pass_obj
def r(app: Mona, remotes: str, args: List[str]) -> None:
    """Execute a Mona command on a remote."""
    for remote in app.parse_remotes(remotes):
        if args[0] in {'init', 'run', 'dispatch'}:
            remote.update()
        remote.command(args)
