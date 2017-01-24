from pathlib import Path
import os
import io
import tarfile
from base64 import b64encode
from itertools import takewhile
import imp
from textwrap import dedent
import hashlib
import subprocess as sp
from configparser import ConfigParser

from caflib.Utils import get_timestamp, cd, config_items, groupby
from caflib.Timing import timing
from caflib.Logging import error, dep_error, info, Table, colstr
from caflib.CLI import CLI, CLIExit
from caflib.Cellar import Cellar
from caflib.Remote import Remote, Local
from caflib.Configure import Context
from caflib.Scheduler import Scheduler

try:
    from docopt import docopt, DocoptExit
except ImportError:
    dep_error('docopt')


def import_cscript(unpack):
    cscript = imp.new_module('cscript')
    try:
        with open('cscript.py') as f:
            script = f.read()
    except FileNotFoundError:
        error('Cscript does not exist.')
    for i in range(2):
        try:
            exec(compile(script, 'cscript', 'exec'), cscript.__dict__)
        except Exception as e:
            if isinstance(e, ImportError) and i == 0:
                unpack(None, path=None)
            else:
                import traceback
                traceback.print_exc()
                error('There was an error while reading cscript.')
    return cscript


class Caf(CLI):
    def __init__(self):
        super().__init__('caf')
        self.cafdir = Path('.caf')
        self.config = ConfigParser()
        self.config.read([
            self.cafdir/'config.ini',
            os.path.expanduser('~/.config/caf/config.ini')
        ])
        with timing('read cscript'):
            self.cscript = import_cscript(self.commands[('unpack',)]._func)
        self.out = Path(getattr(self.cscript, 'out', 'build'))
        self.top = Path(getattr(self.cscript, 'top', '.'))
        self.remotes = {
            name: Remote(r['host'], r['path'], self.top)
            for name, r in config_items(self.config, 'remote')
        }
        self.remotes['local'] = Local()

    def __call__(self, argv):
        if not self.cafdir.is_dir():
            self.cafdir.mkdir()
            info(f'Initializing an empty repository in {self.cafdir.resolve()}.')
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
        rargv = [argv[0], args['COMMAND']] + args['ARGS']  # remote argv
        try:  # try CLI as will be seen on remote
            rargs = self.parse(rargv)
        except DocoptExit:  # remote CLI failed too, reraise CLIExit
            raise cliexit
        if 'make' in rargs:
            if rargs['--queue']:  # substitute URL
                queue = self.get_queue_url(rargs['--queue'], 'get')
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
        if 'make' in rargs and not rargs['conf'] and not args['--no-check']:
            for remote in remotes:
                remote.check(self.out)
        for remote in remotes:
            remote.command(' '.join(
                arg if ' ' not in arg else repr(arg) for arg in rargv[1:]
            ))
            if 'make' in rargs and rargs['conf'] and not args['--no-check']:
                remote.check(self.out)

    def __format__(self, fmt):
        if fmt == 'header':
            return 'Caf -- Calculation framework.'
        if fmt == 'usage':
            s = """\
            Usage:
                caf COMMAND [ARGS...]
                caf [--no-check] REMOTE COMMAND [ARGS...]
            """.rstrip()
            return dedent(s)
        if fmt == 'options':
            s = """\
            Options:
                --no-check           Do not check remote cellar.
            """.rstrip()
            return dedent(s)
        return super().__format__(fmt)

    def get_queue_url(self, queue, action):
        if 'queue' in self.conf:
            if action == 'submit':
                q = self.conf['queue'].get(queue)
                if q:
                    return f'{q["host"]}/token/{q["token"]}/submit'
            elif action == 'get':
                queue, qid = queue.split(':', 1)
                q = self.conf['queue'].get(queue)
                if q:
                    return f'{q["host"]}/token/{q["token"]}/queue/{qid}/get'
            elif action == 'append':
                queue, qid = queue.split(':', 1)
                q = self.conf['queue'].get(queue)
                if q:
                    return f'{q["host"]}/token/{q["token"]}/queue/{qid}/append'

    def proc_remote(self, remotes):
        if remotes == 'all':
            remotes = self.remotes.values()
        else:
            try:
                remotes = [self.remotes[r] for r in remotes.split(',')]
            except KeyError as e:
                error(f'Remote "{e.args[0]}" is not defined')
        return remotes


def get_leafs(conf):
    leafs = {}
    queue = list(conf['targets'].items())
    while queue:
        target, taskid = queue.pop()
        if conf['hashes'][taskid]:
            leafs[target] = conf['hashes'][taskid]
        else:
            for name, child in conf['tasks'][taskid]['children'].items():
                queue.append((f'{target}/{name}', child))
    return leafs


@Caf.command()
def conf(caf, dry: '--dry'):
    """
    Usage:
        caf conf [--dry]

    Options:
        -n, --dry                  Dry run (do not write to disk).
    """
    if not hasattr(caf.cscript, 'configure'):
        error('cscript has to contain function configure(ctx)')
    if not (caf.cafdir/'objects').exists():
        if 'cache' in caf.config['core']:
            ts = get_timestamp()
            path = Path(caf.config['core']['cache'])/f'{Path.cwd().name}_{ts}'
            path.mkdir()
            (caf.cafdir/'objects').symlink_to(path)
    cellar = Cellar(caf.cafdir)
    ctx = Context('.', cellar)
    with timing('evaluate cscript'):
        caf.cscript.configure(ctx)
    with timing('sort tasks'):
        ctx.sort_tasks()
    if dry:
        return
    with timing('configure'):
        inputs = ctx.process()
    with timing('get configuration'):
        conf = ctx.get_configuration()
    targets = get_leafs(conf)
    tasks = {
        hashid: {
            **task,
            'children': {
                name: conf['hashes'][child]
                for name, child in task['children'].items()
            }
        }
        for hashid, task in zip(conf['hashes'], conf['tasks'])
        if 'command' in task
    }
    with timing('store build'):
        tasks = cellar.store_build(tasks, targets, inputs)
    scheduler = Scheduler(caf.cafdir)
    scheduler.submit(tasks)


@Caf.command(triggers=['conf make'])
def make(caf, profile: '--profile', n: ('-j', int), targets: 'TARGET',
         limit: ('--limit', int), queue: '--queue', myid: '--id',
         dry: '--dry', do_conf: 'conf', verbose: '--verbose',
         last_queue: '--last', maxdepth: ('--maxdepth', int)):
    """
    Execute all prepared build tasks.

    Usage:
        caf [conf] make [-v] [--limit N]
                        [--profile PROFILE [-j N] | [--id ID] [--dry]]
                        [--last | --queue URL | [TARGET...] [--maxdepth N]]

    Options:
        -n, --dry                  Dry run (do not write to disk).
        --id ID                    ID of worker [default: 1].
        -p, --profile PROFILE      Run worker via ~/.config/caf/worker_PROFILE.
        -q, --queue URL            Take tasks from web queue.
        --last                     As above, but use the last submitted queue.
        -j N                       Number of launched workers [default: 1].
        -l, --limit N              Limit number of tasks to N.
        -v, --verbose              Be more verbose.
        --maxdepth N               Maximal depth.
    """
    if do_conf:
        conf(['caf', 'conf'], caf)
    scheduler = Scheduler(caf.cafdir)
    for task in scheduler.tasks():
        with cd(task.path):
            with open('run.out', 'w') as stdout, open('run.err', 'w') as stderr:
                try:
                    sp.check_call(task.command, shell=True, stdout=stdout, stderr=stderr)
                except sp.CalledProcessError as exc:
                    task.error(exc)
                else:
                    task.done()
    # if profile:
    #     for _ in range(n):
    #         cmd = [
    #             f'{os.environ["HOME"]}/.config/caf/worker_{profile}',
    #             '-v' if verbose else None,
    #             ('--limit', limit),
    #             ('--queue', queue),
    #             targets,
    #             ('--maxdepth', maxdepth)
    #         ]
    #         try:
    #             sp.check_call(filter_cmd(cmd))
    #         except sp.CalledProcessError:
    #             error(f'Running ~/.config/caf/worker_{profile} did not succeed.')
    # else:
    #     if queue or last_queue:
    #         if last_queue:
    #             with open('.caf/LAST_QUEUE') as f:
    #                 queue = f.read().strip()
    #         url = caf.get_queue_url(queue, 'get')
    #         worker = QueueWorker(
    #             myid, caf.cache, url, dry=dry, limit=limit, debug=verbose
    #         )
    #     else:
    #         if targets:
    #             roots = [caf.out/t for t in targets]
    #         else:
    #             targets = caf.out.glob('*')
    #         tasks = OrderedDict()
    #         for path in find_tasks(*roots, unsealed=True, maxdepth=maxdepth):
    #             cellarid = get_stored(path)
    #             if cellarid not in tasks:
    #                 tasks[cellarid] = str(path)
    #         worker = LocalWorker(
    #             myid,
    #             caf.cellar.workplace,
    #             list(reversed(tasks.items())),
    #             dry=dry,
    #             limit=limit,
    #             debug=verbose
    #         )
    #     worker.work()


@Caf.command()
def checkout(caf, path: '--path'):
    """
    Usage:
        caf checkout [-p PATH]

    Options:
        -p, --path PATH          Where to checkout [default: build].
    """
    cellar = Cellar(caf.cafdir)
    cellar.checkout(path)


# @Caf.command(triggers=['conf submit'])
# def submit(caf, targets: 'TARGET', queue: 'URL', maxdepth: ('--maxdepth', int),
#            do_conf: 'conf'):
#     """
#     Submit the list of prepared tasks to a queue server.
#
#     Usage:
#         caf [conf] submit URL [TARGET...] [--maxdepth N]
#
#     Options:
#         --maxdepth N             Maximum depth.
#     """
#     from urllib.request import urlopen
#     if do_conf:
#         conf(['caf', 'conf'], caf)
#     url = caf.get_queue_url(queue, 'submit')
#     roots = [caf.out/t for t in targets] \
#         if targets else (caf.out).glob('*')
#     tasks = OrderedDict()
#     for path in find_tasks(*roots, unsealed=True, maxdepth=maxdepth):
#         cellarid = get_stored(path)
#         if cellarid not in tasks:
#             tasks[cellarid] = path
#     if not tasks:
#         error('No tasks to submit')
#     data = '\n'.join('{} {}'.format(label, h)
#                      for h, label in reversed(tasks.items())).encode()
#     with urlopen(url, data=data) as r:
#         queue_url = r.read().decode()
#         print('./caf make --queue {}'.format(queue_url))
#     with open('.caf/LAST_QUEUE', 'w') as f:
#         f.write(queue_url)


# @Caf.command()
# def append(caf, targets: 'TARGET', queue: 'URL', maxdepth: ('--maxdepth', int)):
#     """
#     Append the list of prepared tasks to a given queue.
#
#     Usage:
#         caf append URL [TARGET...] [--maxdepth N]
#
#     Options:
#         --maxdepth N             Maximum depth.
#     """
#     from urllib.request import urlopen
#     url = caf.get_queue_url(queue, 'append')
#     roots = [caf.out/t for t in targets] \
#         if targets else (caf.out).glob('*')
#     tasks = OrderedDict()
#     for path in find_tasks(*roots, unsealed=True, maxdepth=maxdepth):
#         cellarid = get_stored(path)
#         if cellarid not in tasks:
#             tasks[cellarid] = path
#     if not tasks:
#         error('No tasks to submit')
#     data = '\n'.join('{} {}'.format(label, h)
#                      for h, label in reversed(tasks.items())).encode()
#     with urlopen(url, data=data) as r:
#         queue_url = r.read().decode()
#         print('./caf make --queue {}'.format(queue_url))
#     with open('.caf/LAST_QUEUE', 'w') as f:
#         f.write(queue_url)


# @Caf.command()
# def reset(caf, targets: 'TARGET'):
#     """
#     Remove working lock and error on tasks.
#
#     Usage:
#         caf reset [TARGET...]
#     """
#     roots = [caf.out/t for t in targets] if targets else (caf.out).glob('*')
#     for path in find_tasks(*roots):
#         if (path/'.lock').is_dir():
#             (path/'.lock').rmdir()
#         if (path/'.caf/error').is_file():
#             (path/'.caf/error').unlink()


caf_list = CLI('list', header='List various entities.')
Caf.commands[('list',)] = caf_list


@caf_list.add_command(name='profiles')
def list_profiles(caf, _):
    """
    List profiles.

    Usage:
        caf list profiles
    """
    for p in Path.home().glob('.config/caf/worker_*'):
        print(p.name)


# @caf_list.add_command(name='remotes')
# def list_remotes(caf, _):
#     """
#     List remotes.
#
#     Usage:
#         caf list remotes
#     """
#     remote_conf = Configuration()
#     remote_conf.update(caf.conf.get('remotes', {}))
#     print(remote_conf)


# @caf_list.add_command(name='tasks')
# def list_tasks(caf, _, do_finished: '--finished', do_stored: '--stored',
#                do_error: '--error', do_unfinished: '--unfinished',
#                in_cellar: '--cellar', both_paths: '--both',
#                maxdepth: ('--maxdepth', int), targets: 'TARGET'):
#     """
#     List tasks.
#
#     Usage:
#         caf list tasks [TARGET...] [--finished | --stored | --error | --unfinished]
#                        [--cellar | --both] [--maxdepth N]
#
#     Options:
#         --finished                 List finished tasks.
#         --unfinished               List unfinished tasks.
#         --stored                   List stored tasks.
#         --error                    List tasks in error.
#         --cellar                   Print path in cellar.
#         --both                     Print path in build and cellar.
#         --maxdepth N               Specify maximum depth.
#     """
#     roots = [caf.out/t for t in targets] if targets else (caf.out).glob('*')
#     if do_finished:
#         paths = find_tasks(*roots, sealed=True, maxdepth=maxdepth)
#     elif do_unfinished:
#         paths = find_tasks(*roots, unsealed=True, maxdepth=maxdepth)
#     elif do_stored:
#         paths = find_tasks(*roots, stored=True, maxdepth=maxdepth)
#     elif do_error:
#         paths = find_tasks(*roots, error=True, maxdepth=maxdepth)
#     else:
#         paths = find_tasks(*roots, maxdepth=maxdepth)
#     if in_cellar:
#         for path in paths:
#             print(get_stored(path, require=False))
#     elif both_paths:
#         for path in paths:
#             print(path, get_stored(path, require=False))
#     else:
#         for path in paths:
#             print(path)


# @Caf.command()
# def search(caf, older: '--older', contains: '--contains',
#            contains_not: '--contains-not'):
#     """
#     Search within stored tasks.
#
#     Usage:
#         caf search [--contains PATTERN] [--contains-not PATTERN] [--older TIME]
#
#     Options:
#         --contains PATTERN         Search tasks containing PATTERN.
#         --contains-not PATTERN     Search tasks not containing PATTERN.
#         --older TIME               Search tasks older than.
#     """
#     cmd = ['find', str(caf.cellar), '-maxdepth', '3',
#            '-mindepth', '3', '-type', 'd']
#     if older:
#         lim = older
#         if lim[0] not in ['-', '+']:
#             lim = '+' + lim
#         cmd.extend(['-ctime', lim])
#     if contains:
#         cmd.extend(['-exec', 'test', '-e', '{{}}/{}'.format(contains), ';'])
#     if contains_not:
#         cmd.extend(['!', '-exec', 'test', '-e', '{{}}/{}'.format(contains_not), ';'])
#     cmd.append('-print')
#     sp.call(cmd)


@Caf.command()
def status(caf):
    """
    Print number of initialized, running and finished tasks.

    Usage:
        caf status
    """
    cellar = Cellar(caf.cafdir)
    scheduler = Scheduler(caf.cafdir)
    states = dict(scheduler.execute('select * from queue'))
    cellar.execute('drop table if exists current_tasks')
    cellar.execute(
        'create temporary table current_tasks(taskhash text, state integer)'
    )
    cellar.executemany('insert into current_tasks values (?,?)', (
        states.items()
    ))
    states = cellar.execute(
        'select path, state from current_tasks join '
        '(select taskhash, path from targets join '
        '(select id from builds order by created desc limit 1) b '
        'on targets.buildid = b.id) build '
        'on current_tasks.taskhash = build.taskhash'
    ).fetchall()
    colors = 'yellow green red normal'.split()
    print('number of {} tasks:'.format('/'.join(
        colstr(s, color) for s, color in zip(
            'running finished error all'.split(),
            colors
        )
    )))
    table = Table(
        align=['<', *len(colors)*['>']],
        sep=[' ', *(len(colors)-1)*['/']]
    )
    for root, group in groupby(states, key=lambda r: r[0].split('/', 1)[0]):
        grouped = {
            state: subgroup for state, subgroup
            in groupby(group, key=lambda r: r[1])
        }
        stats = [len(grouped.get(state, [])) for state in [2, 1, -1]]
        stats.append(len(group))
        stats = [
            colstr(s, color) if s else colstr(s, 'normal')
            for s, color in zip(stats, colors)
        ]
        table.add_row(root + ':', *stats)
        for path, _ in grouped.get(2, []):
            table.add_row(f"{colstr('>>', 'yellow')} {path}", free=True)
    print(table)


@Caf.command()
def cmd(caf, cmd: 'CMD'):
    """
    Execute any shell command.

    Usage:
        caf cmd CMD

    This is a simple convenience alias for running commands remotely.
    """
    sp.call(cmd, shell=True)


caf_remote = CLI('remote', header='Manage remotes.')
Caf.commands[('remote',)] = caf_remote


@caf_remote.add_command(name='add')
def remote_add(caf, _, url: 'URL', name: 'NAME'):
    """
    Add a remote.

    Usage:
        caf remote add URL [NAME]
    """
    config = ConfigParser()
    config.read([caf.cafdir/'config.ini'])
    host, path = url.split(':')
    name = name or host
    config[f'remote "{name}"'] = {'host': host, 'path': path}
    with (caf.cafdir/'config.ini').open('w') as f:
        config.write(f)


@caf_remote.add_command(name='path')
def remote_path(caf, _, name: 'NAME'):
    """
    Print a remote path in the form HOST:PATH.

    Usage:
        caf remote path NAME
    """
    print('{0[host]}:{0[path]}'.format(caf.config[f'remote "{name}"']))


# @Caf.command()
# def update(caf, delete: '--delete', remotes: ('REMOTE', 'proc_remote')):
#     """
#     Sync the contents of . to remote excluding .caf/db and ./build.
#
#     Usage:
#         caf update REMOTE [--delete]
#
#     Options:
#         --delete                   Delete files when syncing.
#     """
#     for remote in remotes:
#         remote.update(delete=delete)


# @Caf.command()
# def check(caf, remotes: ('REMOTE', 'proc_remote')):
#     """
#     Verify that hashes of the local and remote tasks match.
#
#     Usage:
#         caf check REMOTE
#     """
#     for remote in remotes:
#         remote.check(caf.out)


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


# @Caf.command()
# def fetch(caf, dry: '--dry', targets: 'TARGET', remotes: ('REMOTE', 'proc_remote'),
#           get_all: '--all', follow: '--follow', only_mark: '--mark'):
#     """
#     Fetch targets from remote and store them in local Cellar.
#
#     Usage:
#         caf fetch REMOTE [TARGET...] [--dry] [--all] [--follow] [--mark]
#
#     Options:
#         -n, --dry         Dry run (do not write to disk).
#         --all             Do not check which tasks are finished.
#         --follow          Follow dependencies.
#         --mark            Do not really fetch, only mark with remote seals.
#     """
#     for remote in remotes:
#         remote.fetch(targets, caf.cache, caf.out, dry=dry, get_all=get_all, follow=follow, only_mark=only_mark)


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
    strip(['caf', 'strip'], caf)
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
        f.write(f'# version: {version}\n')
        f.write(f'# archive: {b64encode(archive).decode()}\n')
        f.write('# <==\n')
