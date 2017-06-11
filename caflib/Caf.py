# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
import os
import shutil
import sys
import subprocess as sp
from configparser import ConfigParser
import signal
import json

from .Utils import get_timestamp, cd, config_items, groupby, listify
from .Logging import error, info, Table, colstr, warn, no_cafdir, \
    handle_broken_pipe
from . import Logging
from .CLI import Arg, define_cli, CLI, CLIError
from .Cellar import Cellar, State, Hash, TPath
from .Remote import Remote, Local
from .Configure import Context, get_configuration
from .Scheduler import RemoteScheduler, Scheduler
from .Announcer import Announcer

from typing import (  # noqa
    Any, Union, Dict, List, Optional, Set, Iterable, Sequence
)
from types import ModuleType


def import_cscript() -> Union[ModuleType, object]:
    try:
        import cscript
    except ModuleNotFoundError:
        return object()
    return cscript


class RemoteNotExists(Exception):
    pass


class Caf:
    def __init__(self) -> None:
        self.cafdir = Path('.caf')
        self.config = ConfigParser()
        self.config.read([  # type: ignore
            self.cafdir/'config.ini',
            Path('~/.config/caf/config.ini').expanduser()
        ])
        self.cscript = import_cscript()
        self.out = Path(getattr(self.cscript, 'out', 'build'))
        self.top = Path(getattr(self.cscript, 'top', '.'))
        self.paths = listify(getattr(self.cscript, 'paths', []))
        self.remotes = {
            name: Remote(r['host'], r['path'], self.top)
            for name, r in config_items(self.config, 'remote')
        }
        self.remotes['local'] = Local()
        self.cli = CLI([
            ('conf', conf),
            ('make', make),
            ('checkout', checkout),
            ('submit', submit),
            ('reset', reset),
            ('list', [
                ('profiles', list_profiles),
                ('remotes', list_remotes),
                ('builds', list_builds),
                ('task', list_tasks),
            ]),
            ('status', status),
            ('gc', gc),
            ('cmd', cmd),
            ('remote', [
                ('add', remote_add),
                ('path', remote_path),
                ('list', list_remotes),
            ]),
            ('update', update),
            ('check', check),
            ('fetch', fetch),
            ('archive', [
                ('save', archive_store),
            ]),
            ('go', go),
        ])

    def __call__(self, args: List[str] = sys.argv[1:]) -> Any:
        if self.cafdir.exists():
            with (self.cafdir/'log').open('a') as f:
                f.write(f'{get_timestamp()}: {" ".join(args)}\n')
        try:
            return self.cli.run(args)
        except CLIError as e:
            clierror = e
        else:
            return
        remote_spec, *args = args
        try:
            kwargs = self.cli.parse(args)
        except CLIError as e:
            rclierror: Optional[CLIError] = e
        else:
            rclierror = None
        try:
            remotes = self.parse_remotes(remote_spec)
        except RemoteNotExists as e:
            if not rclierror and kwargs:
                error(f'Remote {e.args[0]!r} is not defined')
            else:
                clierror.reraise()
        else:
            if rclierror:
                rclierror.reraise()
        args = self.get_remote_args(args, kwargs)
        remotes = self.parse_remotes(remote_spec)
        if args[0] in ['conf', 'make']:
            for remote in remotes:
                remote.update()
        if args[0] == 'make':
            check(self, remote_spec)
        for remote in remotes:
            remote.command(' '.join(
                arg if ' ' not in arg else repr(arg) for arg in args
            ))

    def parse_remotes(self, remotes: str) -> List[Remote]:
        if remotes == 'all':
            return [r for r in self.remotes.values() if not isinstance(r, Local)]
        try:
            return [self.remotes[r] for r in remotes.split(',')]
        except KeyError:
            pass
        raise RemoteNotExists(remotes)

    def get_remote_args(self, args: List[str], kwargs: Dict) -> List[str]:
        args = args.copy()
        if '--last' in args:
            idx = args.index('--last')
            args = args[:idx] + ['--queue', self.last_queue] + args[idx+1:]
        if args[0] == 'make' and 'url' in kwargs:
            queue = self.get_queue_url(kwargs['url'])
            args = [
                arg if arg != kwargs['url'] else queue for arg in args
            ]
        return args

    @property
    def last_queue(self) -> str:
        try:
            return (self.cafdir/'LAST_QUEUE').read_text()
        except FileNotFoundError:
            error('No queue was ever submitted, cannot use --last')

    @last_queue.setter
    def last_queue(self, queue: str) -> None:
        (self.cafdir/'LAST_QUEUE').write_text(queue)

    def get_queue_url(self, queue: str) -> str:
        num: Optional[str]
        if ':' in queue:
            queue_name, num = queue.rsplit(':', 1)
        else:
            queue_name, num = queue, None
        queue_sec = f'queue "{queue_name}"'
        if not self.config.has_section(queue_sec):
            return queue
        queue_conf = self.config[queue_sec]
        host = queue_conf['host']
        token = queue_conf['token']
        queue = f'{host}/token/{token}'
        if num:
            queue += f'/queue/{num}'
        return queue


@define_cli()
def conf(caf: Caf) -> None:
    """Prepare tasks: process cscript.py and store tasks in cellar."""
    try:
        caf.cscript.run  # type: ignore
    except AttributeError:
        error('cscript has to contain function run()')
    if not caf.cafdir.is_dir():
        caf.cafdir.mkdir()
        info(f'Initializing an empty repository in {caf.cafdir.resolve()}.')
        if caf.config.has_option('core', 'cache'):
            ts = get_timestamp()
            path = Path(caf.config['core']['cache'])/f'{Path.cwd().name}_{ts}'
            path.mkdir()
            (caf.cafdir/'objects').symlink_to(path)
        else:
            (caf.cafdir/'objects').mkdir()
    cellar = Cellar(caf.cafdir)
    ctx = Context(caf.top, cellar, conf_only=True)
    try:
        caf.cscript.run(ctx)  # type: ignore
    except Exception as e:
        import traceback
        traceback.print_exc()
        error('There was an error when executing run()')
    conf = get_configuration(ctx.tasks, ctx.targets.values())
    tasks = dict(cellar.store_build(conf.tasks, conf.targets, ctx.inputs, conf.labels))
    labels: Dict[Hash, Optional[TPath]] = {hashid: None for hashid in tasks}
    for tpath, hashid in cellar.get_tree(hashes=tasks.keys()).items():
        if not labels[hashid]:
            labels[hashid] = tpath
    if any(label is None for label in labels.values()):
        warn('Some tasks are not accessible.')
    taskdefs = [(hashid, state, labels[hashid] or '?') for hashid, state in tasks.items()]
    scheduler = Scheduler(caf.cafdir)
    scheduler.submit(taskdefs)


def sig_handler(sig: Any, frame: Any) -> Any:
    print(f'Received signal {signal.Signals(sig).name}')
    raise KeyboardInterrupt


@define_cli([
    Arg('patterns', metavar='PATTERN', nargs='*', help='Tasks to be built'),
    Arg('-l', '--limit', type=int, help='Limit number of tasks to N'),
    Arg('-p', '--profile', help='Run worker via ~/.config/caf/worker_PROFILE'),
    Arg('-j', dest='n', type=int, help='Number of launched workers [default: 1]'),
    Arg('-q', '--queue', dest='url', help='Take tasks from web queue'),
    Arg('--last', action='store_true', help='Use last submitted queue'),
    Arg('-v', '--verbose', action='store_true'),
    Arg('--maxerror', type=int, help='Number of errors in row to quit [default: 5]'),
    Arg('-r', '--random', action='store_true', help='Pick tasks in random order')
])
def make(caf: Caf,
         patterns: List[str],
         limit: int = None,
         profile: str = None,
         n: int = 1,
         url: str = None,
         dry: bool = False,
         last: bool = False,
         verbose: bool = False,
         maxerror: int = 5,
         randomize: bool = False) -> None:
    """Execute build tasks."""
    if profile:
        cmd = [os.path.expanduser(f'~/.config/caf/worker_{profile}')]
        if verbose:
            cmd.append('-v')
        if randomize:
            cmd.append('-r')
        if dry:
            cmd.append('--dry')
        if limit:
            cmd.extend(('-l', str(limit)))
        if maxerror:
            cmd.extend(('--maxerror', str(maxerror)))
        if url:
            cmd.extend(('-q', url))
        elif last:
            cmd.extend(('-q', caf.last_queue))
        cmd.extend(patterns)
        for _ in range(n):
            try:
                sp.run(cmd, check=True)
            except sp.CalledProcessError:
                error(f'Running ~/.config/caf/worker_{profile} did not succeed.')
        return
    if verbose:
        Logging.DEBUG = True
    if url:
        url = caf.get_queue_url(url)
        scheduler: Scheduler = RemoteScheduler(
            url,
            caf.config.get('core', 'curl', fallback='') or None,
            caf.cafdir,
            tmpdir=caf.config.get('core', 'tmpdir', fallback='') or None,
        )
    else:
        scheduler = Scheduler(
            caf.cafdir,
            tmpdir=caf.config.get('core', 'tmpdir', fallback='') or None,
        )
    if patterns:
        cellar = Cellar(caf.cafdir)
        hashes: Optional[Set[Hash]] = \
            set(hashid for hashid, _ in cellar.get_tree().glob(*patterns))
        if not hashes:
            return
    else:
        hashes = None
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGXCPU, sig_handler)
    for task in scheduler.tasks_for_work(
            hashes=hashes, limit=limit, dry=dry, nmaxerror=maxerror,
            randomize=randomize
    ):
        with cd(task.path):
            with open('run.out', 'w') as stdout, open('run.err', 'w') as stderr:
                try:
                    sp.run(
                        task.command,
                        shell=True,
                        stdout=stdout,
                        stderr=stderr,
                        check=True
                    )
                except sp.CalledProcessError as exc:
                    task.error(str(exc))
                except KeyboardInterrupt:
                    task.interrupt()
                else:
                    task.done()


@define_cli([
    Arg('patterns', metavar='PATTERN', nargs='*',
        help='Tasks to be checked out'),
    Arg('-b', '--blddir', type=Path, help=f'Where to checkout [default: blddir]'),
    Arg('--json', dest='do_json', action='store_true',
        help='Do not checkout, print JSONs of hashes from STDIN.'),
    Arg('-f', '--force', action='store_true', help='Remove PATH if exists'),
    Arg('-n', dest='nth', type=int, help='Nth build to the past'),
    Arg('--finished', action='store_true', help='Check out only finished tasks'),
    Arg('-L', '--no-link', action='store_true',
        help='Do not create links to cellar, but copy'),
])
def checkout(caf: Caf,
             blddir: Path = Path('build'),
             patterns: Iterable[str] = None,
             do_json: bool = False,
             force: bool = False,
             nth: int = 0,
             finished: bool = False,
             no_link: bool = False) -> None:
    """Create the dependecy tree physically on a file system."""
    cellar = Cellar(caf.cafdir)
    if not do_json:
        if blddir.exists():
            if force:
                shutil.rmtree(blddir)
            else:
                error(f'Cannot checkout to existing path: {blddir}')
        cellar.checkout(
            blddir, patterns=patterns or ['**'], nth=nth, finished=finished,
            nolink=no_link
        )
    else:
        hashes = [Hash(l.strip()) for l in sys.stdin.readlines()]
        json.dump({
            hashid: task.asdict() for hashid, task in
            cellar.get_tasks(hashes).items()
        }, sys.stdout)


@define_cli([
    Arg('url', metavar='URL'),
    Arg('patterns', metavar='PATTERN', nargs='*', help='Tasks to be submitted'),
    Arg('-a', '--append', action='store_true', help='Append to an existing queue'),
])
def submit(caf: Caf, patterns: List[str], url: str, append: bool = False) -> None:
    """Submit the list of prepared tasks to a queue server."""
    url = caf.get_queue_url(url)
    announcer = Announcer(url, caf.config.get('core', 'curl', fallback='') or None)
    scheduler = Scheduler(caf.cafdir)
    queue = scheduler.get_queue()
    if patterns:
        cellar = Cellar(caf.cafdir)
        hashes = dict(cellar.get_tree().glob(*patterns))
    else:
        hashes = {hashid: TPath(label) for hashid, (state, label, *_) in queue.items()}
    hashes = {
        hashid: label for hashid, label in hashes.items()
        if queue[hashid][0] == State.CLEAN
    }
    if not hashes:
        error('No tasks to submit')
    queue_url = announcer.submit(hashes, append=append)
    if queue_url:
        print(f'./caf make --queue {queue_url}')
        caf.last_queue = queue_url


@define_cli([
    Arg('patterns', metavar='PATTERN', nargs='*', help='Tasks to be reset'),
    Arg('--running', action='store_true', help='Also reset running tasks'),
    Arg('--hard', action='store_true',
        help='Also reset finished tasks and remove outputs'),
])
def reset(caf: Caf, patterns: List[str], hard: bool, running: bool) -> None:
    """Remove all temporary checkouts and set tasks to clean."""
    if hard and input('Are you sure? ["y" to confirm] ') != 'y':
        return
    if hard:
        running = True
    cellar = Cellar(caf.cafdir)
    scheduler = Scheduler(caf.cafdir)
    states = scheduler.get_states()
    queue = scheduler.get_queue()
    if patterns:
        hashes = set(
            hashid for hashid, _
            in cellar.get_tree(hashes=states.keys()).glob(*patterns)
        )
    else:
        hashes = set(queue)
    for hashid in hashes:
        if states[hashid] in (State.ERROR, State.INTERRUPTED) \
                or running and states[hashid] == State.RUNNING:
            scheduler.reset_task(hashid)
        elif hard and states[hashid] in (State.DONE, State.DONEREMOTE, State.CLEAN):
            scheduler.reset_task(hashid)
            if states[hashid] in (State.DONE, State.CLEAN):
                cellar.reset_task(hashid)


@define_cli()
def list_profiles(caf: Caf) -> None:
    """List profiles."""
    for p in Path.home().glob('.config/caf/worker_*'):
        print(p.name)


@define_cli()
def list_remotes(caf: Caf) -> None:
    """List remotes."""
    for name, remote in config_items(caf.config, 'remote'):
        print(name)
        print(f'\t{remote["host"]}:{remote["path"]}')


@define_cli()
def list_builds(caf: Caf) -> None:
    """List builds."""
    cellar = Cellar(caf.cafdir)
    table = Table(align='<<')
    for i, created in reversed(list(enumerate(cellar.get_builds()))):
        table.add_row(str(i), created)
    print(table)


@define_cli([
    Arg('patterns', metavar='PATTERN', nargs='*', help='Tasks to be listed'),
    Arg('--finished', dest='do_finished', action='store_true',
        help='List finished tasks'),
    Arg('--unfinished', dest='do_unfinished', action='store_true',
        help='List unfinished tasks'),
    Arg('--running', dest='do_running', action='store_true',
        help='List running tasks'),
    Arg('--error', dest='do_error', action='store_true',
        help='List tasks in error'),
    Arg('--hash', dest='disp_hash', action='store_true',
        help='Display task hash'),
    Arg('--path', dest='disp_path', action='store_true',
        help='Display task virtual path'),
    Arg('--tmp', dest='disp_tmp', action='store_true',
        help='Display temporary path'),
    Arg('--no-color', dest='no_color', action='store_true',
        help='Do not color paths')
])
def list_tasks(caf: Caf,
               patterns: List[str],
               do_finished: bool = False,
               do_unfinished: bool = False,
               do_running: bool = False,
               do_error: bool = False,
               disp_hash: bool = False,
               disp_path: bool = False,
               disp_tmp: bool = False,
               no_color: bool = False) -> None:
    """List tasks."""
    cellar = Cellar(caf.cafdir)
    scheduler = Scheduler(caf.cafdir)
    states = scheduler.get_states()
    queue = scheduler.get_queue()
    if patterns:
        hashes_paths = cellar.get_tree(hashes=states.keys()).glob(*patterns)
    else:
        hashes_paths = (
            (hashid, label) for hashid, (_, label, *__) in sorted(
                queue.items(), key=lambda r: r[1]
            )
        )
    for hashid, path in hashes_paths:
        if do_finished and states[hashid] not in (State.DONE, State.DONEREMOTE):
            continue
        if do_error and states[hashid] != State.ERROR:
            continue
        if do_unfinished and states[hashid] in (State.DONE, State.DONEREMOTE):
            continue
        if do_running and states[hashid] != State.RUNNING:
            continue
        if not no_color:
            pathstr = colstr(path, states[hashid].color)
        if disp_hash:
            line: str = hashid
        elif disp_tmp:
            if queue[hashid][2]:
                line = queue[hashid][2]
            else:
                continue
        elif disp_path:
            line = pathstr
        else:
            line = f'{hashid} {pathstr} {queue[hashid][2] or ""}'
        try:
            sys.stdout.write(line + '\n')
        except BrokenPipeError:
            handle_broken_pipe()
            break


@define_cli([
    Arg('patterns', metavar='PATTERN', nargs='*', help='Tasks to be reset'),
    Arg('-i', '--incomplete', action='store_true',
        help='Print only incomplete patterns'),
])
def status(caf: Caf, patterns: List[str] = None, incomplete: bool = False) -> None:
    """Print number of initialized, running and finished tasks."""
    cellar = Cellar(caf.cafdir)
    scheduler = Scheduler(caf.cafdir)
    patterns = patterns or caf.paths
    colors = 'yellow green cyan red normal'.split()
    print('number of {} tasks:'.format('/'.join(
        colstr(s, color) for s, color in zip(
            'running finished remote error all'.split(),
            colors
        )
    )))
    states = scheduler.get_states()
    tree = cellar.get_tree(hashes=states.keys())
    groups = tree.dglob(*patterns)
    queue = scheduler.get_queue()
    groups['ALL'] = [(hashid, label) for hashid, (_, label, *__) in queue.items()]
    table = Table(
        align=['<', *len(colors)*['>']],
        sep=['   ', *(len(colors)-1)*['/']]
    )
    for pattern, hashes_paths in groups.items():
        if not hashes_paths:
            pattern = colstr(pattern, 'bryellow')
        grouped = {
            state: subgroup for state, subgroup
            in groupby(hashes_paths, key=lambda x: states[x[0]])
        }
        stats: List[Any] = [len(grouped.get(state, [])) for state in (
            State.RUNNING,
            State.DONE,
            State.DONEREMOTE,
            State.ERROR
        )]
        stats.append(len(hashes_paths))
        if incomplete and stats[1] + stats[2] == stats[4] and pattern != 'All':
            continue
        stats = [
            colstr(s, color) if s else colstr(s, 'normal')
            for s, color in zip(stats, colors)
        ]
        table.add_row(pattern, *stats)
    for state in (State.RUNNING, State.INTERRUPTED):
        color = state.color
        for hashid, path in grouped.get(state, []):
            table.add_row(
                f"{colstr('>>', color)} {path} "
                f"{colstr(queue[hashid][2], color)} {queue[hashid][3]}",
                free=True
            )
    print(table)


@define_cli([
    Arg('-a', '--all', action='store_true', help='Discard all nonactive tasks'),
])
def gc(caf: Caf, gc_all: bool = False) -> None:
    """Discard running and error tasks."""
    scheduler = Scheduler(caf.cafdir)
    scheduler.gc()
    if gc_all:
        scheduler.gc_all()
        cellar = Cellar(caf.cafdir)
        cellar.gc()


@define_cli([
    Arg('cmd', metavar='CMD',
        help='This is a simple convenience alias for running commands remotely'),
])
def cmd(caf: Caf, cmd: str) -> None:
    """Execute any shell command."""
    sp.run(cmd, shell=True)


@define_cli([
    Arg('url', metavar='URL'),
    Arg('name', metavar='NAME', nargs='?')
])
def remote_add(caf: Caf, url: str, name: str = None) -> None:
    """Add a remote."""
    config = ConfigParser(interpolation=None)
    config.read([caf.cafdir/'config.ini'])  # type: ignore
    host, path = url.split(':')
    name = name or host
    config[f'remote "{name}"'] = {'host': host, 'path': path}
    try:
        with (caf.cafdir/'config.ini').open('w') as f:
            config.write(f)
    except FileNotFoundError:
        no_cafdir()


@define_cli([
    Arg('name', metavar='NAME')
])
def remote_path(caf: Caf, _: Any, name: str) -> None:
    """Print a remote path in the form HOST:PATH."""
    print('{0[host]}:{0[path]}'.format(caf.config[f'remote "{name}"']))


@define_cli([
    Arg('remotes', metavar='REMOTE'),
    Arg('--delete', action='store_true', help='Delete files when syncing'),
])
def update(caf: Caf, remotes: str, delete: bool = False) -> None:
    """Update a remote."""
    for remote in caf.parse_remotes(remotes):
        remote.update(delete=delete)


@define_cli([
    Arg('remotes', metavar='REMOTE'),
])
def check(caf: Caf, remotes: str) -> None:
    """Verify that hashes of the local and remote tasks match."""
    scheduler = Scheduler(caf.cafdir)
    hashes = {
        label: hashid for hashid, (_, label, *__) in scheduler.get_queue().items()
    }
    for remote in caf.parse_remotes(remotes):
        remote.check(hashes)


@define_cli([
    Arg('remotes', metavar='REMOTE'),
    Arg('patterns', metavar='PATTERN', nargs='*', help='Tasks to fetch'),
    Arg('--no-files', action='store_true', help='Fetch task metadata, but not files'),
])
def fetch(caf: Caf,
          patterns: List[str],
          remotes: str,
          no_files: bool = False) -> None:
    """Fetch targets from remote."""
    cellar = Cellar(caf.cafdir)
    scheduler = Scheduler(caf.cafdir)
    states = scheduler.get_states()
    if patterns:
        hashes = set(hashid for hashid, _ in cellar.get_tree().glob(*patterns))
    else:
        hashes = set(states)
    for remote in caf.parse_remotes(remotes):
        tasks = remote.fetch([
            hashid for hashid in hashes if states[hashid] == State.CLEAN
        ] if no_files else [
            hashid for hashid in hashes
            if states[hashid] in (State.CLEAN, State.DONEREMOTE)
        ], files=not no_files)
        for hashid, task in tasks.items():
            if not no_files:
                cellar.seal_task(hashid, hashed_outputs=task['outputs'])
            scheduler.task_done(hashid, remote=remote.host if no_files else None)


@define_cli([
    Arg('filename', metavar='FILE'),
    Arg('patterns', metavar='PATTERN', nargs='*'),
])
def archive_store(caf: Caf, filename: str, patterns: List[str]) -> None:
    """Archives files accessible from the given tasks as tar.gz."""
    cellar = Cellar(caf.cafdir)
    scheduler = Scheduler(caf.cafdir)
    states = scheduler.get_states()
    if patterns:
        hashes = set(hashid for hashid, _ in cellar.get_tree().glob(*patterns))
    else:
        hashes = set(states)
    cellar.archive(hashes, filename)  # type: ignore  # TODO


# @Caf.command()
# def push(caf, targets: 'TARGET', dry: '--dry', remotes: ('REMOTE', 'proc_remote')):
#     """
#     Push targets to remote and store them in remote Cellar.
#
#     Usage:
#         caf push REMOTE [TARGET...] [--dry]
#
#     Options:
#         -n, --dry                  Dry run (do not write to disk).
#     """
#     for remote in remotes:
#         remote.push(targets, caf.cache, caf.out, dry=dry)


@define_cli([
    Arg('remotes', metavar='REMOTE'),
])
def go(caf: Caf, remotes: str) -> None:
    """SSH into the remote caf repository."""
    for remote in caf.parse_remotes(remotes):
        remote.go()
