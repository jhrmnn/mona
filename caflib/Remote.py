import subprocess
from pathlib import Path
import glob
from caflib.Logging import info


def upload(host, path):
    info('Uploading to {}...'.format(host))
    subprocess.check_call(['ssh', host, 'mkdir -p {}'.format(path)])
    subprocess.check_call(['rsync',
                           '-ia',
                           '--exclude=.*',
                           '--exclude=build',
                           '--exclude=_caf',
                           '--exclude=*.pyc',
                           '--exclude=__pycache__',
                           '.',
                           '{}:{}'.format(host, path)])


def command(cmd, host, path):
    info('Connecting to {}...'.format(host))
    subprocess.check_call(['ssh', host,
                           'cd {} && ./caf {}'.format(path, cmd)])


def fetch(targets, cellar, build, host, path):
    info('Downloading from {}...'.format(host))
    if targets:
        targets = [build/t for t in targets]
    else:
        targets = [Path(p) for p in glob.glob('{}/*'.format(build))]
    paths = set()
    for target in targets:
        for task in target.glob('*'):
            task = task.resolve()
            assert task.parts[-4] == 'Cellar'
            paths.add('/'.join(task.parts[-3:]))
    p = subprocess.Popen(['rsync',
                          '-iar',
                          '--files-from=-',
                          '{}:{}/{}'.format(host, path, cellar),
                          str(cellar)],
                         stdin=subprocess.PIPE)
    p.communicate('\n'.join(paths).encode())
