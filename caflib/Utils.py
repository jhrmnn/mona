import subprocess
from pathlib import Path
import yaml
import re
import os
from contextlib import contextmanager
from datetime import datetime
import json


def normalize_str(s):
    return re.sub(r'[^0-9a-zA-Z.-]', '-', s)


def slugify(x):
    if isinstance(x, tuple):
        s = '_'.join(normalize_str(str(x)) for x in x)
    elif isinstance(x, dict):
        s = '_'.join('{}={}'.format(normalize_str(str(k)),
                                    normalize_str(str(v)))
                     for k, v in x.items())
    else:
        s = str(x)
    return s


def get_timestamp():
    return format(datetime.today(), '%Y-%m-%d_%H:%M:%S')


def mkdir(path, parents=False):
    command = ['mkdir', str(path)]
    if parents:
        command.insert(1, '-p')
    p = subprocess.Popen(command, stderr=subprocess.PIPE)
    _, stderr = p.communicate()
    if p.returncode:
        stderr = stderr.decode()
        m = re.match(r'mkdir: (.*): No such file or directory\n', stderr)
        if m:
            raise FileNotFoundError(m.group().strip())
        raise OSError(stderr)
    return path


def build_cmd(*args):
    cmd = ''
    for arg in args:
        if isinstance(arg, tuple):
            option, value = arg
            if value:
                cmd += ' {} {}'.format(option, value)
        else:
            cmd += ' {}'.format(arg)
    return cmd


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


class ArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return obj.tolist()
        except AttributeError:
            return super().default(obj)


class Configuration:
    def __init__(self, path):
        self.path = Path(path)
        self._dict = {}
        self.load()

    def __getitem__(self, key):
        return self._dict.get(key, None)

    def __setitem__(self, key, val):
        self._dict[key] = val

    def __contains__(self, x):
        return x in self._dict

    def load(self):
        if self.path.is_file():
            with self.path.open() as f:
                self._dict = yaml.load(f)

    def save(self):
        if not self.path.parent.is_dir():
            mkdir(self.path.parent)
        with self.path.open('w') as f:
            yaml.dump(self._dict, f)
