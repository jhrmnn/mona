# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
import os
import shutil
import sys
from textwrap import dedent
import subprocess as sp
from configparser import ConfigParser
import signal
import json

from .Utils import get_timestamp, cd, config_items, groupby, listify
from .Logging import error, info, Table, colstr, warn, no_cafdir, \
    handle_broken_pipe
from . import Logging
from .CLI import CLI, CLIExit
from .CLI2 import Arg, define_cli
from .Cellar import Cellar, State, Hash, TPath
from .Remote import Remote, Local
from .Configure import Context, get_configuration
from .Scheduler import RemoteScheduler, Scheduler
from .Announcer import Announcer

from docopt import docopt, DocoptExit

from typing import (  # noqa
    Any, Union, Dict, List, Optional, Set, Iterable
)
from types import ModuleType


def import_cscript() -> Union[ModuleType, object]:
    try:
        import cscript
    except ModuleNotFoundError:
        return object()
    return cscript


class Caf(CLI):
    def __init__(self) -> None:
        super().__init__('caf')
        self.cafdir = Path('.caf')
        self.config = ConfigParser()
        self.config.read([  # type: ignore
            self.cafdir/'config.ini',
            os.path.expanduser('~/.config/caf/config.ini')
        ])
        self.cscript = import_cscript()
        self.out = Path(getattr(self.cscript, 'out', 'build'))
        self.top = Path(getattr(self.cscript, 'top', '.'))
        self.paths = listify(getattr(self.cscript, 'paths', []))
        self.remotes: Dict[str, Union[Local, Remote]] = {
            name: Remote(r['host'], r['path'], self.top)
            for name, r in config_items(self.config, 'remote')
        }
        self.remotes['local'] = Local()

    def __call__(self, argv: List[str], *_: CLI) -> None:
        if self.cafdir.exists():
            with (self.cafdir/'log').open('a') as f:
                f.write(f'{get_timestamp()}: {" ".join(argv)}\n')
        try:
            super().__call__(argv)  # try CLI as if local
        except CLIExit as e:  # store exception for reraise if remote fails too
            cliexit = e
        else:
            return
        # the local CLI above did not succeed, make a usage without local CLI
        usage = '\n'.join(
            l for l in str(self).splitlines() if 'caf COMMAND' not in l
        )
        try:  # parse local
            args = docopt(usage, argv=argv[1:], options_first=True, help=False)
        except DocoptExit:  # remote CLI failed too, reraise CLIExit
            raise cliexit
        rargv: List[str] = [argv[0], args['COMMAND']] + args['ARGS']  # remote argv
        try:  # try CLI as will be seen on remote
            rargs = self.parse(rargv)
        except DocoptExit:  # remote CLI failed too, reraise CLIExit
            raise cliexit
        if 'make' in rargs:
            # this substitues only locally known values
            if rargs['--queue']:  # substitute URL
                queue = self.get_queue_url(rargs['--queue'])
                rargv = [
                    arg if arg != rargs['--queue'] else queue for arg in rargv
                ]
            elif rargs['--last']:
                with (self.cafdir/'LAST_QUEUE').open() as f:
                    queue_url = f.read().strip()
                last_index = rargv.index('--last')
                rargv = rargv[:last_index] + ['--queue', queue_url] \
                    + rargv[last_index+1:]
        remotes = self.proc_remote(args['REMOTE'])  # get Remote objects
        if args['COMMAND'] in ['conf', 'make']:
            for remote in remotes:
                remote.update()
        if 'make' in rargs:
            for remote in remotes:
                self.commands[('check',)]._func(self, remotes)  # type: ignore
        for remote in remotes:
            remote.command(' '.join(
                arg if ' ' not in arg else repr(arg) for arg in rargv[1:]
            ))

    def __format__(self, fmt: str) -> str:
        if fmt == 'header':
            return 'Caf -- Calculation framework.'
        if fmt == 'usage':
            s = """\
            Usage:
                caf COMMAND [ARGS...]
                caf REMOTE COMMAND [ARGS...]
            """.rstrip()
            return dedent(s)
        return super().__format__(fmt)

    def proc_remote(self, remotes: str) -> List[Union[Remote, Local]]:
        if remotes == 'all':
            rems: List[Union[Remote, Local]] = [
                r for r in self.remotes.values() if not isinstance(r, Local)
            ]
        else:
            try:
                rems = [self.remotes[r] for r in remotes.split(',')]
            except KeyError as e:
                error(f'Remote "{e.args[0]}" is not defined')
        return rems

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


@Caf.command()
def conf(caf: Caf) -> None:
    """
    Prepare tasks -- process cscript.py and store tasks in cellar.

    Usage:
        caf conf
    """
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


@Caf.command(mapping=dict(
    profile='--profile', n=('-j', int), patterns='PATH', limit=('--limit', int),
    url='--queue', dry='--dry', verbose='--verbose', _='--last',
    maxerror=('--maxerror', int), randomize='--random'))
def make(caf: Caf,
         profile: str,
         n: int,
         patterns: List[str],
         limit: int,
         url: str,
         dry: bool,
         verbose: bool,
         _: Any,
         maxerror: int,
         randomize: bool) -> None:
    """
    Execute build tasks.

    Usage:
        caf make [PATH...] [-v] [--dry] [-p PROFILE [-j N]] [-q URL | --last | -r]
                 [-l N] [--maxerror N]

    Options:
        -l, --limit N              Limit number of tasks to N.
        -p, --profile PROFILE      Run worker via ~/.config/caf/worker_PROFILE.
        -j N                       Number of launched workers [default: 1].
        -n, --dry                  Dry run (do not actually work on tasks).
        -q, --queue URL            Take tasks from web queue.
        --last                     As above, but use the last submitted queue.
        -v, --verbose              Be verbose.
        --maxerror N               Number of errors in row to quit [default: 5].
        -r, --random               Pick tasks in random order.
    """
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


@Caf.command(mapping=dict(
    blddir=('--blddir', Path), patterns='PATH', do_json='--json', force='--force',
    nth=('-n', int), finished='--finished', no_link='--no-link'))
@define_cli([
    Arg('patterns', metavar='PATTERN', nargs='*',
        help='Tasks to be checked out'),
    Arg('-b', '--blddir', type=Path, default='blddir',
        help=f'Where to checkout [default: blddir]'),
    Arg('--json', dest='do_json', action='store_true',
        help='Do not checkout, print JSONs of hashes from STDIN.'),
    Arg('-f', '--force', action='store_true', help='Remove PATH if exists'),
    Arg('-n', dest='nth', type=int, help='Nth build to the past'),
    Arg('--finished', action='store_true', help='Check out only finished tasks'),
    Arg('-L', '--no-link', action='store_true',
        help='Do not create links to cellar, but copy'),
])
def checkout(caf: Caf,
             blddir: Path,
             patterns: Iterable[str] = None,
             do_json: bool = False,
             force: bool = False,
             nth: int = 0,
             finished: bool = False,
             no_link: bool = False) -> None:
    """
    Create the dependecy tree physically on a file system.

    Usage:
        caf checkout [-b PATH | --json] [--no-link] [PATH...] [-f] [-n N] [--finished]

    Options:
        -b --blddir PATH     Where to checkout [default: build].
        --json              Do not checkout, print JSONs of hashes from STDIN.
        -f, --force         Remove PATH if exists.
        -n N                Nth build to the past [default: 0].
        --finished          Check out only finished tasks.
        -L, --no-link       Do not create links to cellar, but copy.
    """
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


@Caf.command(mapping=dict(patterns='PATH', url='URL', append='--append'))
def submit(caf: Caf, patterns: List[str], url: str, append: bool) -> None:
    """
    Submit the list of prepared tasks to a queue server.

    Usage:
        caf submit URL [PATH...] [-a]

    Options:
        -a, --append        Append to an existing queue.
    """
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
        with (caf.cafdir/'LAST_QUEUE').open('w') as f:
            f.write(queue_url)


@Caf.command(mapping=dict(patterns='PATH', hard='--hard', running='--running'))
def reset(caf: Caf, patterns: List[str], hard: bool, running: bool) -> None:
    """
    Remove all temporary checkouts and set tasks to clean.

    Usage:
        caf reset [PATH...] [--running] [--hard]

    Options:
        --running       Also reset running tasks.
        --hard          Also reset finished tasks and remove outputs.
    """
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


caf_list = CLI('list', header='List various entities.')
Caf.commands[('list',)] = caf_list


@caf_list.add_command(name='profiles')
def list_profiles(caf: Caf, _: Any) -> None:
    """
    List profiles.

    Usage:
        caf list profiles
    """
    for p in Path.home().glob('.config/caf/worker_*'):
        print(p.name)


@caf_list.add_command(name='remotes')
def list_remotes(caf: Caf, _: Any) -> None:
    """
    List remotes.

    Usage:
        caf list remotes
    """
    for name, remote in config_items(caf.config, 'remote'):
        print(name)
        print(f'\t{remote["host"]}:{remote["path"]}')


@caf_list.add_command(name='builds')
def list_builds(caf: Caf, _: Any) -> None:
    """
    List builds.

    Usage:
        caf list builds
    """
    cellar = Cellar(caf.cafdir)
    table = Table(align='<<')
    for i, created in reversed(list(enumerate(cellar.get_builds()))):
        table.add_row(str(i), created)
    print(table)


@caf_list.add_command(name='tasks', mapping=dict(
    do_finished='--finished', do_running='--running', do_error='--error',
    do_unfinished='--unfinished', disp_hash='--hash', disp_path='--path',
    patterns='PATH', disp_tmp='--tmp', no_color='--no-color'))
def list_tasks(caf: Caf, _: Any, do_finished: bool, do_running: bool,
               do_error: bool, do_unfinished: bool, disp_hash: bool,
               disp_path: bool, patterns: List[str], disp_tmp: bool,
               no_color: bool) -> None:
    """
    List tasks.

    Usage:
        caf list tasks [PATH...] [--finished | --error | --unfinished | --running]
                       [--hash | --path | --tmp] [--no-color]

    Options:
        --finished          List finished tasks.
        --unfinished        List unfinished tasks.
        --error             List tasks in error.
        --hash              Display task hash.
        --path              Display task virtual path.
        --tmp               Display temporary path.
        --no-color          Do not color paths.
    """
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


@Caf.command(mapping=dict(patterns='PATH', incomplete='--incomplete'))
def status(caf: Caf, patterns: List[str], incomplete: bool) -> None:
    """
    Print number of initialized, running and finished tasks.

    Usage:
        caf status [PATH...] [-i]

    Options:
        -i, --incomplete      Print only incomplete patterns.
    """
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


@Caf.command(mapping=dict(gc_all='--all'))
def gc(caf: Caf, gc_all: bool) -> None:
    """
    Discard running and error tasks.

    Usage:
        caf gc [--all]

    Options:
        -a, --all      Discard all nonactive tasks.
    """
    scheduler = Scheduler(caf.cafdir)
    scheduler.gc()
    if gc_all:
        scheduler.gc_all()
        cellar = Cellar(caf.cafdir)
        cellar.gc()


@Caf.command(mapping=dict(cmd='CMD'))
def cmd(caf: Caf, cmd: str) -> None:
    """
    Execute any shell command.

    Usage:
        caf cmd CMD

    This is a simple convenience alias for running commands remotely.
    """
    sp.run(cmd, shell=True)


caf_remote = CLI('remote', header='Manage remotes.')
Caf.commands[('remote',)] = caf_remote


@caf_remote.add_command(name='add', mapping=dict(url='URL', name='NAME'))
def remote_add(caf: Caf, _: Any, url: str, name: str) -> None:
    """
    Add a remote.

    Usage:
        caf remote add URL [NAME]
    """
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


@caf_remote.add_command(name='path', mapping=dict(name='NAME'))
def remote_path(caf: Caf, _: Any, name: str) -> None:
    """
    Print a remote path in the form HOST:PATH.

    Usage:
        caf remote path NAME
    """
    print('{0[host]}:{0[path]}'.format(caf.config[f'remote "{name}"']))


@Caf.command(mapping=dict(delete='--delete', remotes=('REMOTE', 'proc_remote')))
def update(caf: Caf, delete: bool, remotes: List[Union[Remote, Local]]) -> None:
    """
    Update a remote.

    Usage:
        caf update REMOTE [--delete]

    Options:
        --delete                   Delete files when syncing.
    """
    for remote in remotes:
        remote.update(delete=delete)


@Caf.command(mapping=dict(remotes=('REMOTE', 'proc_remote')))
def check(caf: Caf, remotes: List[Union[Remote, Local]]) -> None:
    """
    Verify that hashes of the local and remote tasks match.

    Usage:
        caf check REMOTE
    """
    scheduler = Scheduler(caf.cafdir)
    hashes = {
        label: hashid for hashid, (_, label, *__) in scheduler.get_queue().items()
    }
    for remote in remotes:
        remote.check(hashes)


@Caf.command(mapping=dict(
    patterns='PATH', remotes=('REMOTE', 'proc_remote'), nofiles='--no-files'))
def fetch(caf: Caf, patterns: List[str], remotes: List[Union[Remote, Local]],
          nofiles: bool) -> None:
    """
    Fetch targets from remote.

    Usage:
        caf fetch REMOTE [PATH...] [--no-files]

    Options:
        --no-files          Fetch task metadata, but not files.
    """
    cellar = Cellar(caf.cafdir)
    scheduler = Scheduler(caf.cafdir)
    states = scheduler.get_states()
    if patterns:
        hashes = set(hashid for hashid, _ in cellar.get_tree().glob(*patterns))
    else:
        hashes = set(states)
    for remote in remotes:
        tasks = remote.fetch([
            hashid for hashid in hashes if states[hashid] == State.CLEAN
        ] if nofiles else [
            hashid for hashid in hashes
            if states[hashid] in (State.CLEAN, State.DONEREMOTE)
        ], files=not nofiles)
        for hashid, task in tasks.items():
            if not nofiles:
                cellar.seal_task(hashid, hashed_outputs=task['outputs'])
            scheduler.task_done(hashid, remote=remote.host if nofiles else None)


caf_archive = CLI('archive', header='Cellar archiving.')
Caf.commands[('archive',)] = caf_archive


@caf_archive.add_command(name='store', mapping=dict(filename='FILE', patterns='PATH'))
def archive_store(caf: Caf, _: Any, filename: str, patterns: List[str]) -> None:
    """
    Archives files accessible from the given tasks as tar.gz.

    Usage:
        caf archive save FILE [PATH...]
    """
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


@Caf.command(mapping=dict(remotes=('REMOTE', 'proc_remote')))
def go(caf: Caf, remotes: List[Union[Local, Remote]]) -> None:
    """
    SSH into the remote caf repository.

    Usage:
        caf go REMOTE
    """
    for remote in remotes:
        remote.go()
