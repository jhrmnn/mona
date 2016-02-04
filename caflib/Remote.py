import subprocess
from pathlib import Path
import glob
from caflib.Logging import info, error, warn


class Remote:
    def __init__(self, host, path):
        self.host = host
        self.path = path

    def update(self):
        info('Updating {.host}...'.format(self))
        subprocess.check_call(['ssh', self.host, 'mkdir -p {.path}'.format(self)])
        subprocess.check_call(['rsync',
                               '-cirl',
                               '--exclude=.*',
                               '--exclude=build',
                               '--exclude=_caf',
                               '--exclude=*.pyc',
                               '--exclude=__pycache__',
                               '.',
                               '{0.host}:{0.path}'.format(self)])

    def command(self, cmd):
        info('Running `./caf {}` on {.host}...'.format(cmd, self))
        try:
            subprocess.check_call(['ssh', self.host,
                                   'cd {.path} && exec python3 -u caf {}'.format(self, cmd)])
        except subprocess.CalledProcessError:
            error('Command `{}` on {.host} ended with error'
                  .format(cmd, self))

    def check(self, targets, cellar, batch):
        info('Checking {}...'.format(self.host))
        targets = get_targets(targets, batch)
        paths = set()
        for target in targets:
            for task in [target] if target.is_symlink() else target.glob('*'):
                task_full = task.resolve()
                if task_full.parts[-4] == 'Cellar':
                    paths.add('/'.join(task_full.parts[-3:]))
        try:
            subprocess.check_call(['ssh', self.host,
                                   'cd {0.path}/{1} && ls -d {2} &>/dev/null'
                                   .format(self, cellar, ' '.join(paths))],
                                  stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            error('Local Tasks are not in remote Cellar')
        else:
            info('Local Tasks are in remote Cellar.')

    def fetch(self, targets, cellar, batch, dry=False):
        info('Fetching from {}...'.format(self.host))
        targets = get_targets(targets, batch)
        paths = set()
        for target in targets:
            for task in [target] if target.is_symlink() else target.glob('*'):
                task_full = task.resolve()
                if task_full.parts[-4] == 'Cellar':
                    paths.add('/'.join(task_full.parts[-3:]))
                else:
                    warn('{}: Task has to be in Cellar before fetching'.format(task))
        p = subprocess.Popen(['rsync',
                              '-cirlP',
                              '--exclude=*.pyc',
                              '--exclude=__pycache__'] +
                             (['--dry-run'] if dry else []) +
                             ['--files-from=-',
                              '{0.host}:{0.path}/{1}'.format(self, cellar),
                              str(cellar)],
                             stdin=subprocess.PIPE)
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
                    warn('{}: Task has to be in Cellar for pushing'.format(task))
        p = subprocess.Popen(['rsync',
                              '-cirlP'] +
                             (['--dry-run'] if dry else []) +
                             ['--files-from=-',
                              str(cellar),
                              '{0.host}:{0.path}/{1}'.format(self, cellar)],
                             stdin=subprocess.PIPE)
        p.communicate('\n'.join(paths).encode())

    def go(self):
        subprocess.call(['ssh', '-t', self.host,
                        'cd {.path} && exec $SHELL -l'.format(self)])


def get_targets(targets, batch):
    if targets:
        return [batch/t for t in targets]
    else:
        return [Path(p) for p in glob.glob('{}/*'.format(batch))]
