import subprocess
from pathlib import Path
import yaml
import re
import os
from contextlib import contextmanager


def normalize_str(s):
    return re.sub(r'[^0-9a-zA-Z.-]', '-', s)


def slugify(x):
    if isinstance(x, str):
        s = x
    elif isinstance(x, tuple):
        s = '_'.join(normalize_str(str(x)) for x in x)
    elif isinstance(x, dict):
        s = '_'.join('{}={}'.format(normalize_str(k), normalize_str(v))
                     for k, v in x.items())
    elif x is None:
        return None
    return s


def mkdir(path):
    subprocess.check_call(['mkdir', '-p', str(path)])
    return path


def find_program(cmd):
    return Path(subprocess.check_output(['which', cmd]).decode().strip()).resolve()


@contextmanager
def cd(path):
    path = str(path)
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


def listify(obj):
    if not obj:
        return []
    if isinstance(obj, (str, bytes, tuple)):
        return [obj]
    try:
        return list(obj)
    except TypeError:
        return [obj]


class Configuration:
    def __init__(self, path):
        self.path = Path(path)
        self._dict = {}
        self.load()

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, val):
        self._dict[key] = val

    def load(self):
        if self.path.is_file():
            with self.path.open() as f:
                self._dict = yaml.load(f)

    def save(self):
        mkdir(self.path.parent)
        with self.path.open('w') as f:
            yaml.dump(self._dict, f)
