import subprocess
from pathlib import Path
import glob
from caflib.Logging import info, error
from caflib.Utils import get_files, filter_cmd
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
        cmd = ['rsync',
               '-cirl',
               '--delete' if delete else None,
               '--exclude=.*',
               '--exclude=build',
               '--exclude=_caf',
               '--exclude=*.pyc',
               '--exclude=__pycache__',
               ['--exclude={}'.format(p) for p in ignored],
               ['caf', 'cscript', str(self.top)],
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
                'cd {.path} && exec python3 -u caf {}'.format(self, cmd)])
        except subprocess.CalledProcessError:
            error('Command `{}` on {.host} ended with error'
                  .format(cmd, self))
        return output.strip() if get_output else None

    def check(self, targets, batch):
        info('Checking {}...'.format(self.host))
        here = dict(get_files(batch))
        there = dict(l.split() for l
                     in self.command('list tasks --stored', get_output=True)
                     .decode().split('\n'))
        missing = []
        for task, target in here.items():
            ptask = Path(task)
            ptarget = Path(target)
            if targets and ptask.parts[2] not in targets:
                continue
            if ptarget.parents[2].name == 'Cellar':
                token = str(Path('/'.join(ptarget.parts[-4:])))
                if token != there.get(task):
                    missing.append((task, token, there.get(task)))
        if missing:
            for item in missing:
                print('{}: {} is not {}'.format(*item))
            error('Local Tasks are not in remote Cellar')
        else:
            info('Local Tasks are in remote Cellar.')

    def fetch(self, cellar, batch=None, targets=None, tasks=None, dry=False):
        info('Fetching from {}...'.format(self.host))
        if batch:
            targets = get_targets(targets, batch)
            paths = set()
            for target in targets:
                for task in [target] if target.is_symlink() else target.glob('*'):
                    task_full = task.resolve()
                    if task_full.parts[-4] == 'Cellar':
                        paths.add('/'.join(task_full.parts[-3:]))
                    else:
                        error('{}: Task has to be in Cellar before fetching'.format(task))
        elif tasks:
            paths = []
            for task in tasks:
                path = Path(task).resolve()
                if path.parts[-4] == 'Cellar':
                    paths.append('/'.join(path.parts[-3:]))
                else:
                    error('{}: Task has to be in Cellar before fetching'.format(task))
        cmd = ['rsync',
               '-cirlP',
               '--delete',
               '--exclude=*.pyc',
               '--exclude=.caf/env',
               '--exclude=__pycache__',
               '--dry-run' if dry else None,
               '--files-from=-',
               '{0.host}:{0.path}/{1}'.format(self, cellar),
               str(cellar)]
        p = subprocess.Popen(filter_cmd(cmd), stdin=subprocess.PIPE)
        p.communicate('\n'.join(paths).encode())

    def push(self, targets, cellar, batch, dry=False):
        info('Pushing to {}...'.format(self.host))
        targets = get_targets(targets, batch)
        paths = set()
        for target in targets:
            for task in [target] if target.is_symlink() else target.glob('*'):
                task_full = task.resolve()
                if task_full.parts[-4] == 'Cellar':
                    paths.add('/'.join(task_full.parts[-3:]))
                else:
                    error('{}: Task has to be in Cellar for pushing'.format(task))
        cmd = ['rsync',
               '-cirlP',
               '--delete',
               '--exclude=*.pyc',
               '--exclude=.caf/env',
               '--exclude=__pycache__',
               '--dry-run' if dry else None,
               '--files-from=-',
               str(cellar),
               '{0.host}:{0.path}/{1}'.format(self, cellar)]
        p = subprocess.Popen(filter_cmd(cmd), stdin=subprocess.PIPE)
        p.communicate('\n'.join(paths).encode())

    def go(self):
        subprocess.call(['ssh', '-t', self.host,
                         'cd {.path} && exec $SHELL'.format(self)])


def get_targets(targets, batch):
    if targets:
        return [batch/t for t in targets]
    else:
        return [Path(p) for p in glob.glob('{}/*'.format(batch))]
