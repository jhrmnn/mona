import subprocess
from pathlib import Path
import glob
from caflib.Logging import info, error


class Remote:
    def __init__(self, host, path):
        self.host = host
        self.path = path

    def update(self):
        info('Updating {.host}...'.format(self))
        subprocess.check_call(['ssh', self.host, 'mkdir -p {.path}'.format(self)])
        subprocess.check_call(['rsync',
                               '-ia',
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
                                   'cd {.path} && exec ./caf {}'.format(self, cmd)])
        except subprocess.CalledProcessError:
            error('Command `{}` on {.host} ended with error'
                  .format(cmd, self))

    def fetch(self, targets, cellar, batch, dry=False):
        info('Fetching from {}...'.format(self.host))
        if targets:
            targets = [batch/t for t in targets]
        else:
            targets = [Path(p) for p in glob.glob('{}/*'.format(batch))]
        paths = set()
        for target in targets:
            for task in [target] if target.is_symlink() else target.glob('*'):
                task_full = task.resolve()
                if task_full.parts[-4] != 'Cellar':
                    error('{}: Task has to be in Cellar before fetching'.format(task))
                paths.add('/'.join(task_full.parts[-3:]))
        p = subprocess.Popen(['rsync',
                              '-iar'] +
                             (['--dry-run'] if dry else []) +
                             ['--files-from=-',
                              '{0.host}:{0.path}/{1}'.format(self, cellar),
                              str(cellar)],
                             stdin=subprocess.PIPE)
        p.communicate('\n'.join(paths).encode())

    def go(self):
        subprocess.call(['ssh', '-t', self.host,
                        'cd {.path} && exec $SHELL -l'.format(self)])
