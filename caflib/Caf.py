from itertools import chain
from pathlib import Path
import os
import sys
import subprocess
import io
import tarfile
from base64 import b64encode
from itertools import takewhile
import imp
from textwrap import dedent
import hashlib

from caflib.Utils import Configuration, mkdir, get_timestamp, filter_cmd, \
    timing, relink, cd, print_timing, get_all_tasks_in_dir
from caflib.Logging import error, info, colstr, Table, warn, log_caf, dep_error
from caflib.Context import get_stored
from caflib.CLI import CLI, CLIExit
from caflib.Context import Context
from caflib.Worker import QueueWorker, LocalWorker
from caflib.Remote import Remote

try:
    from docopt import docopt, DocoptExit
except ImportError:
    dep_error('docopt')


latest = 'Latest'
cellar = 'Cellar'
brewery = 'Brewery'


def load_module(pathname):
    path = Path(pathname)
    modulename = path.stem
    module = imp.new_module(modulename)
    try:
        exec(compile(path.open().read(), path.name, 'exec'), module.__dict__)
    except:
        import traceback
        traceback.print_exc()
        raise RuntimeError('Could not load "{}"'.format(pathname))
    return module


class Caf(CLI):
    def __init__(self):
        super().__init__('caf')
        self.conf = Configuration('.caf/conf.yaml')
        self.conf.set_global(Configuration('{}/.config/caf/conf.yaml'
                                           .format(os.environ['HOME'])))
        if Path('cscript').is_file():
            cscriptname = 'cscript'
        elif Path('cscript.py').is_file():
            cscriptname = 'cscript.py'
        else:
            error('There is no cscript')
        with timing('reading cscript'):
            try:
                self.cscript = load_module(cscriptname)
            except RuntimeError:
                error('There was an error while reading cscript.')
        self.out = Path(getattr(self.cscript, 'out', 'build'))
        self.cache = Path(getattr(self.cscript, 'cache', '_caf'))
        self.top = Path(getattr(self.cscript, 'top', '.'))
        self.cellar = self.cache/cellar
        self.brewery = self.cache/brewery
        self.remotes = {name: Remote(r['host'], r['path'], self.top)
                        for name, r in self.conf.get('remotes', {}).items()}

    def __call__(self, argv):
        log_caf(argv)
        cliexit = None
        try:
            super().__call__(argv)
            print_timing()
            return
        except CLIExit as e:
            cliexit = e
        usage = '\n'.join(l for l in str(self).splitlines() if 'caf COMMAND' not in l)
        try:
            args = docopt(usage, argv=argv[1:], options_first=True, help=False)
            rargv = [argv[0], args['COMMAND']] + args['ARGS']
            self.parse(rargv)
            remotes = self.proc_remote(args['REMOTE'])
        except DocoptExit:
            args = None
        if args:
            if args['COMMAND'] in ['init', 'build', 'work']:
                for remote in remotes:
                    remote.update()
            if 'work' in rargv and not args['--no-check']:
                targets = self.commands[('work',)].parse(rargv)['TARGET']
                if 'build' not in rargv:
                    for remote in remotes:
                        remote.check(targets, self.out/latest)
            else:
                targets = None
            for remote in remotes:
                remote.command(' '.join(arg if ' ' not in arg else repr(arg)
                                        for arg in rargv[1:]))
                if 'work' in rargv and not args['--no-check'] \
                        and 'build' in rargv:
                    remote.check(targets, self.out/latest)
        else:
            raise cliexit

    def __format__(self, fmt):
        if fmt == 'header':
            return 'Caf -- Calculation framework.'
        elif fmt == 'usage':
            s = """\
            Usage:
                caf COMMAND [ARGS...]
                caf [--no-check] REMOTE COMMAND [ARGS...]
            """.rstrip()
            return dedent(s)
        elif fmt == 'options':
            s = """\
            Options:
                --no-check           Do not check remote cellar.
            """.rstrip()
            return dedent(s)
        else:
            return super().__format__(fmt)

    def finalize(self, sig, frame):
        print_timing()
        sys.exit()

    def proc_remote(self, remotes):
        if remotes == 'all':
            remotes = self.remotes.values()
        else:
            try:
                remotes = [self.remotes[r] for r in remotes.split(',')]
            except KeyError as e:
                error('Remote "{}" is not defined'.format(e.args[0]))
        return remotes


@Caf.command()
def init(caf):
    """
    Initialize the Caf repository.

    Usage:
        caf init

    By default create directory in ./_caf. If 'cache' is defined in
    ~/.config/caf/conf.yaml, the repository is created there and symlinked to
    ./_caf, otherwise it is created locally.
    """
    if 'cache' in caf.conf:
        timestamp = get_timestamp()
        cache_path = Path(caf.conf['cache'])/'{}_{}'.format(Path().resolve().name, timestamp)
        mkdir(cache_path)
        relink(cache_path, caf.cache, relative=False)
    else:
        cache_path = caf.cache
        if cache_path.exists():
            error('{} exists, cannot overwrite'.format(cache_path))
        mkdir(cache_path)
    info('Initializing an empty repository at {}.'.format(cache_path))
    mkdir(caf.cellar)
    mkdir(caf.brewery)


@Caf.command(triggers=['init build'])
def build(caf, dry: '--dry', do_init: 'init'):
    """
    Prepare tasks and targets defined in cscript.

    Usage:
        caf [init] build [--dry]

    Options:
        -n, --dry                  Dry run (do not write to disk).

    Tasks are created in ./_caf/Brewery/Latest and if their preparation does
    not depened on unfinished tasks, they are prepared and stored in
    ./_caf/Cellar based on their SHA1 hash. Targets (collections of symlinks to
    tasks) are created in ./build/Latest.
    """
    if not hasattr(caf.cscript, 'build'):
        error('cscript has to contain function build(ctx)')
    if do_init:
        init('caf init'.split(), caf)
    ctx = Context(caf.cellar, caf.top)
    with timing('dependency tree'):
        caf.cscript.build(ctx)
    if not dry:
        timestamp = get_timestamp()
        mkdir(caf.brewery/timestamp)
        relink(timestamp, caf.brewery/latest, relative=False)
        mkdir(caf.out/timestamp, parents=True)
        relink(timestamp, caf.out/latest, relative=False)
        with timing('build'):
            ctx.build(caf.brewery/latest)
        with timing('targets'):
            ctx.make_targets(caf.out/latest)
        if hasattr(caf.cscript, 'json'):
            warn('Make sure json is not printing dictionaries in features')


@Caf.command(triggers=['build work', 'init build work'])
def work(caf, profile: '--profile', n: ('-j', int), targets: 'TARGET',
         depth: ('--depth', int), limit: ('--limit', int), queue: '--queue',
         brewery: '--brewery', myid: '--id', dry: '--dry',
         info_start: '--info-start', do_init: 'init', do_build: 'build',
         verbose: '--verbose'):
    """
    Execute all prepared build tasks.

    Usage:
        caf [[init] build] work [-v] [TARGET... | --brewery] [--depth N] [--limit N]
                                [--profile PROFILE [-j N] | [--id ID] [--dry]]
        caf [[init] build] work [-v] [--queue URL] [--info-start] [--limit N]
                                [--profile PROFILE [-j N] | [--id ID] [--dry]]

    Options:
        -n, --dry                  Dry run (do not write to disk).
        --id ID                    ID of worker [default: 1].
        -p, --profile PROFILE      Run worker via ~/.config/caf/worker_PROFILE.
        -q, --queue URL            Take tasks from web queue.
        -j N                       Number of launched workers [default: 1].
        -d, --depth N              Limit depth of descending to children.
        -l, --limit N              Limit number of tasks to N.
        -t, --task                 Change command's context to tasks.
        --brewery                  Work on tasks in Brewery.
        --info-start               Push notification when worker starts.
        -v, --verbose              Be more verbose.
    """
    if do_init:
        build('caf init build'.split(), caf)
    elif do_build:
        build('caf build'.split(), caf)
    if profile:
        for _ in range(n):
            cmd = ['{}/.config/caf/worker_{}'
                   .format(os.environ['HOME'], profile),
                   targets, ('-d', depth), ('-l', limit), ('-q', queue),
                   ('--brewery', brewery), ('--info-start', info_start)]
            try:
                subprocess.check_call(filter_cmd(cmd))
            except subprocess.CalledProcessError:
                error('Running ~/.config/caf/worker_{} did not succeed.'
                      .format(profile))
    else:
        if queue:
            worker = QueueWorker(myid, (caf.cellar).resolve(), queue,
                                 dry=dry, limit=limit, info_start=info_start,
                                 debug=verbose)
        else:
            if brewery:
                path = (caf.brewery/latest).resolve()
                depth = 1
            else:
                path = (caf.out/latest).resolve()
            worker = LocalWorker(myid, path, targets, dry=dry, maxdepth=depth,
                                 limit=limit, debug=verbose)
        worker.work()


@Caf.command()
def submit(caf, do_tasks: '--task', tasks: 'TASK', targets: 'TARGET',
           url: 'URL'):
    """
    Submit the list of prepared tasks to a queue server.

    Usage:
        caf submit URL [TARGET...]
        caf submit URL TASK... --task

    Options:
        -t, --task                 Change command's context to tasks.
    """
    from urllib.request import urlopen
    if do_tasks:
        hashes = [get_stored(task, rel=True) for task in tasks]
    else:
        if targets:
            hashes = [get_stored(path, rel=True)
                      for path in subprocess.check_output([
                          'find', '-H', str(caf.out/latest), '-type', 'l',
                          '-exec', 'test', '-f', '{}/.caf/seal', ';', '-print'
                      ])
                      .decode().split()]
        else:
            hashes = [get_stored(path, rel=True)
                      for path in (caf.brewery/latest).glob('*')
                      if path.is_symlink() and not (path/'.caf/seal').is_file()]
    with urlopen(url, data='\n'.join(hashes).encode()) as r:
        print('./caf work --queue {}'.format(r.read().decode()))


@Caf.command()
def reset(caf, targets: 'TARGET'):
    """
    Remove working lock and error on tasks.

    Usage:
        caf reset [TARGET...]
    """
    if targets:
        paths = map(Path, subprocess.check_output([
            'find', '-H', str(caf.out/latest), '-type', 'l'])
            .decode().split())
    else:
        paths = [p for p in (caf.brewery/latest).glob('*') if p.is_symlink()]
    for p in paths:
        if (p/'.lock').is_dir():
            (p/'.lock').rmdir()
        if (p/'.caf/error').is_file():
            (p/'.caf/error').unlink()


caf_list = CLI('list', header='List various entities.')
Caf.commands[('list',)] = caf_list


@caf_list.add_command(name='profiles')
def list_profiles(caf, _):
    """
    List profiles.

    Usage:
        caf list profiles
    """
    for p in Path(os.environ['HOME']).glob('.config/caf/worker_*'):
        print(p.name)


@caf_list.add_command(name='remotes')
def list_remotes(caf, _):
    """
    List remotes.

    Usage:
        caf list remotes
    """
    remote_conf = Configuration()
    remote_conf.update(caf.conf.get('remotes', {}))
    print(remote_conf)


@caf_list.add_command(name='tasks')
def list_tasks(caf, _, do_finished: '--finished', do_stored: '--stored',
               do_error: '--error', do_unfinished: '--unfinished'):
    """
    List tasks.

    Usage:
        caf list tasks [--finished | --stored | --error | --unfinished]

    Options:
        --finished                 List finished tasks.
        --unfinished               List unfinished tasks.
        --stored                   List stored tasks.
        --error                    List tasks in error.
    """
    if do_finished or do_unfinished:
        for buildpath, _ in get_all_tasks_in_dir(caf.out/latest):
            if (buildpath/'.caf/seal').is_file() == do_finished:
                print(buildpath)
    elif do_stored:
        for buildpath, cellarpath in get_all_tasks_in_dir(caf.out/latest):
            if cellarpath.parents[2].name == cellar:
                print(buildpath, '/'.join(cellarpath.parts[-4:]))
    elif do_error:
        for buildpath, _ in get_all_tasks_in_dir(caf.out/latest):
            if (buildpath/'.caf/error').is_file():
                print(buildpath)
    else:
        for buildpath, _ in get_all_tasks_in_dir(caf.out/latest):
            print(buildpath)


@Caf.command()
def search(caf, older: '--older', contains: '--contains',
           contains_not: '--contains-not'):
    """
    Search within stored tasks.

    Usage:
        caf search [--contains PATTERN] [--contains-not PATTERN] [--older TIME]

    Options:
        --contains PATTERN         Search tasks containing PATTERN.
        --contains-not PATTERN     Search tasks not containing PATTERN.
        --older TIME               Search tasks older than.
    """
    cmd = ['find', str(caf.cellar), '-maxdepth', '3',
           '-mindepth', '3', '-type', 'd']
    if older:
        lim = older
        if lim[0] not in ['-', '+']:
            lim = '+' + lim
        cmd.extend(['-ctime', lim])
    if contains:
        cmd.extend(['-exec', 'test', '-e', '{{}}/{}'.format(contains), ';'])
    if contains_not:
        cmd.extend(['!', '-exec', 'test', '-e', '{{}}/{}'.format(contains_not), ';'])
    cmd.append('-print')
    subprocess.call(cmd)


@Caf.command()
def status(caf, targets: 'TARGET'):
    """
    Print number of initialized, running and finished tasks.

    Usage:
        caf status [TARGET...]
    """
    def colored(stat):
        colors = 'blue green red yellow normal'.split()
        return [colstr(s, color) if s else colstr(s, 'normal')
                for s, color in zip(stat, colors)]

    dirs = []
    if not targets:
        dirs.append((caf.brewery/latest, (caf.brewery/latest).glob('*')))
    targets = [caf.out/latest/t for t in targets] \
        if targets else (caf.out/latest).glob('*')
    for target in targets:
        if not target.is_dir() or str(target).startswith('.'):
            continue
        if target.is_symlink():
            dirs.append((target, [target]))
        else:
            dirs.append((target, target.glob('*')))
    print('number of {} tasks:'
          .format('/'.join(colored('running finished error prepared all'.split()))))
    table = Table(align=['<', *5*['>']], sep=[' ', *4*['/']])
    for directory, paths in sorted(dirs):
        stats = []
        locked = []
        for p in paths:
            stats.append(((p/'.lock').is_dir(), (p/'.caf/seal').is_file(),
                          (p/'.caf/error').is_file(), (p/'.caf/lock').is_file(),
                          (p/'.caf').is_dir()))
            if (p/'.lock').is_dir():
                locked.append(p)
        stats = colored([stat.count(True) for stat in zip(*stats)])
        table.add_row(str(directory) + ':', *stats)
        if directory.parts[1] != 'Brewery':
            for path in locked:
                table.add_row('{} {}'.format(colstr('>>', 'blue'), path), free=True)
    print(table)


@Caf.command()
def cmd(caf, do_tasks: '--task', do_print: '--print', targets: 'TARGET',
        cmd: 'CMD'):
    """
    Execute any shell command.

    Usage:
        caf cmd CMD
        caf cmd CMD [TARGET...] --task [--print]

    Options:
        -t, --task                 Change command's context to tasks.
        --print                    Print path to task before running a command.

    This is a simple convenience alias for running commands remotely.
    """
    if do_tasks:
        if targets:
            paths = chain.from_iterable((
                [caf.out/latest/target] if (caf.out/latest/target).is_symlink()
                else (caf.out/latest/target).glob('*')
                for target in targets))
        else:
            paths = (p for p in (caf.brewery/latest).glob('*') if p.is_symlink())
        for path in paths:
            if do_print:
                info('Running `{}` in {}'.format(cmd, path))
            with cd(path):
                subprocess.call(cmd, shell=True)
    else:
        subprocess.call(cmd, shell=True)


caf_remote = CLI('remote', header='Manage remotes.')
Caf.commands[('remote',)] = caf_remote


@caf_remote.add_command(name='add')
def remote_add(caf, _, url: 'URL', name: 'NAME'):
    """
    Add a remote.

    Usage:
        caf remote add URL [NAME]
    """
    host, path = url.split(':')
    name = name or host
    if 'remotes' not in caf.conf:
        caf.conf['remotes'] = {}
    caf.conf['remotes'][name] = {'host': host, 'path': path}
    caf.conf.save()


@Caf.command()
def update(caf, delete: '--delete', remotes: ('REMOTE', 'proc_remote')):
    """
    Sync the contents of . to remote excluding ./_caf and ./build.

    Usage:
        caf update REMOTE [--delete]

    Options:
        --delete                   Delete files when syncing.
    """
    for remote in remotes:
        remote.update(delete=delete)


@Caf.command()
def check(caf, targets: 'TARGET', remotes: ('REMOTE', 'proc_remote')):
    """
    Verify that hashes of the local and remote tasks match.

    Usage:
        caf check REMOTE [TARGET...]
    """
    for remote in remotes:
        remote.check(targets, caf.out/latest)


@Caf.command()
def push(caf, targets: 'TARGET', dry: '--dry', remotes: ('REMOTE', 'proc_remote')):
    """
    Push targets to remote and store them in remote Cellar.

    Usage:
        caf push REMOTE [TARGET...] [--dry]

    Options:
        -n, --dry                  Dry run (do not write to disk).
    """
    for remote in remotes:
        remote.push(targets, caf.cellar, caf.out/latest, dry)


@Caf.command()
def fetch(caf, do_tasks: '--task', dry: '--dry', tasks: 'TASK',
          targets: 'TARGET', remotes: ('REMOTE', 'proc_remote')):
    """
    Fetch targets from remote and store them in local Cellar.

    Usage:
        caf fetch REMOTE [TASK...] --task [--dry]
        caf fetch REMOTE [TARGET...] [--dry]

    Options:
        -n, --dry                  Dry run (do not write to disk).
        -t, --task                 Change command's context to tasks.
    """
    for remote in remotes:
        if do_tasks:
            remote.fetch(caf.cellar, tasks=tasks, dry=dry)
        else:
            remote.fetch(caf.cellar, batch=caf.out/latest,
                         targets=targets, dry=dry)


@Caf.command()
def go(caf, remotes: ('REMOTE', 'proc_remote')):
    """
    SSH into the remote caf repository.

    Usage:
        caf go REMOTE
    """
    for remote in remotes:
        remote.go()


@Caf.command()
def strip(caf):
    """
    Strip packed caflib from the caf executable.

    Usage:
        caf strip
    """
    with open('caf') as f:
        lines = takewhile(lambda l: l != '# ==>\n', f.readlines())
    with open('caf', 'w') as f:
        for line in lines:
            f.write(line)


@Caf.command()
def pack(caf):
    """
    Pack caflib into the caf executable.

    Usage:
        caf pack
    """
    strip('caf strip'.split(), caf)
    h = hashlib.new('md5')
    with io.BytesIO() as ftar:
        archive = tarfile.open(mode='w|gz', fileobj=ftar)
        for path in sorted(Path('caflib').glob('**/*.py')):
            archive.add(str(path))
            with path.open('rb') as f:
                h.update(f.read())
        archive.close()
        archive = ftar.getvalue()
    version = h.hexdigest()
    with open('caf', 'a') as f:
        f.write('# ==>\n')
        f.write('# version: {}\n'.format(version))
        f.write('# archive: {}\n'.format(b64encode(archive).decode()))
        f.write('# <==\n')
