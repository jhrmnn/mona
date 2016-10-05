import subprocess
from pathlib import Path
from caflib.Logging import info, error
from caflib.Utils import filter_cmd
from caflib.Listing import find_tasks
from caflib.Context import get_stored
import os


class Remote:
    def __init__(self, host, path, top):
        self.host = host
        self.path = path
        self.top = top

    def update(self, delete=False):
        info('Updating {.host}...'.format(self))
        subprocess.check_call(['ssh', self.host, 'mkdir -p {.path}'.format(self)])
        ignorefile = Path('.cafignore')
        if ignorefile.is_file():
            ignored = [l.strip() for l in ignorefile.open().readlines()]
        else:
            ignored = []
        ignorefile = Path(os.environ['HOME'] + '/.config/caf/ignore')
        if ignorefile.is_file():
            ignored.extend(l.strip() for l in ignorefile.open().readlines())
        if Path('cscript').is_file():
            cscriptname = 'cscript'
        else:
            cscriptname = 'cscript.py'
        cmd = ['rsync',
               '-cirl',
               '--copy-unsafe-links',
               '--delete' if delete else None,
               '--exclude=.*',
               '--exclude=build',
               '--exclude=_caf',
               '--exclude=*.pyc',
               '--exclude=__pycache__',
               ['--exclude={}'.format(p) for p in ignored],
               ['caf', cscriptname, str(self.top)],
               'caflib' if Path('caflib').exists() else None,
               '{0.host}:{0.path}'.format(self)]
        subprocess.check_call(filter_cmd(cmd))

    def command(self, cmd, get_output=False):
        if not get_output:
            info('Running `./caf {}` on {.host}...'.format(cmd, self))
        caller = subprocess.check_output if get_output else subprocess.check_call
        try:
            output = caller([
                'ssh', '-t', '-o', 'LogLevel=QUIET',
                self.host,
                'sh -c "cd {.path} && exec python3.5 -u caf {}"'.format(self, cmd)])
        except subprocess.CalledProcessError:
            error('Command `{}` on {.host} ended with error'
                  .format(cmd, self))
        return output.strip() if get_output else None

    def check(self, root):
        info('Checking {}...'.format(self.host))
        here = {}
        for path in find_tasks(root):
            cellarpath = get_stored(path, require=None)
            if cellarpath:
                here[str(path)] = str(cellarpath)
        there = dict(l.split() for l
                     in self.command('list tasks --stored --both', get_output=True)
                     .decode().strip().split('\n'))
        missing = []
        for task, target in here.items():
            if target != there.get(task):
                missing.append((task, target, there.get(task)))
        if missing:
            for item in missing:
                print('{}: {} is not {}'.format(*item))
            error('Local Tasks are not in remote Cellar')
        else:
            info('Local Tasks are in remote Cellar.')

    def fetch(self, targets, cache, root, dry=False, get_all=False, follow=False):
        info('Fetching from {}...'.format(self.host))
        if not get_all:
            there = self.command(
                'list tasks {} --finished --cellar {}'.format(
                    ' '.join(targets) if targets else '',
                    '--maxdepth 1' if not follow else ''
                ),
                get_output=True
            ).decode().split('\r\n')
        roots = [p for p in root.glob('*')
                 if not targets or p.name in targets]
        paths = set()
        for task in find_tasks(*roots, stored=True, follow=follow):
            cellarpath = get_stored(task)
            if get_all or cellarpath in there:
                paths.add(cellarpath)
        cmd = ['rsync',
               '-cirlP',
               '--delete',
               '--exclude=*.pyc',
               '--exclude=.caf/env',
               '--exclude=__pycache__',
               '--dry-run' if dry else None,
               '--files-from=-',
               '{0.host}:{0.path}/{1}'.format(self, cache),
               str(cache)]
        p = subprocess.Popen(filter_cmd(cmd), stdin=subprocess.PIPE)
        p.communicate('\n'.join(paths).encode())

    def push(self, targets, cache, root, dry=False):
        info('Pushing to {}...'.format(self.host))
        roots = [p for p in root.glob('*')
                 if not targets or p.name in targets]
        paths = set()
        for task in find_tasks(*roots, stored=True, follow=False):
            paths.add(get_stored(task))
        cmd = ['rsync',
               '-cirlP',
               '--delete',
               '--exclude=*.pyc',
               '--exclude=.caf/env',
               '--exclude=__pycache__',
               '--dry-run' if dry else None,
               '--files-from=-',
               str(cache),
               '{0.host}:{0.path}/{1}'.format(self, cache)]
        p = subprocess.Popen(filter_cmd(cmd), stdin=subprocess.PIPE)
        p.communicate('\n'.join(paths).encode())

    def go(self):
        subprocess.call(['ssh', '-t', self.host,
                         'cd {.path} && exec $SHELL'.format(self)])
