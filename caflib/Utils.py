import subprocess
from pathlib import Path
import yaml
import re
import os
from contextlib import contextmanager
from datetime import datetime
from collections import defaultdict
import time
import json
import itertools
import sys
import stat

from caflib.Logging import Table


_dotiming = 'TIMING' in os.environ
_timing = defaultdict(float)
_timing_stack = []
_writable = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_reports = []


def report(f):
    """Register function as a report in Context.

    Example:

        @report
        def my_report(...
    """
    _reports.append(f)
    return f


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
    s = s.replace('/', '_')
    return s


def get_timestamp():
    return format(datetime.today(), '%Y-%m-%d_%H:%M:%S')


def get_files(batch):
    return sorted([tuple(w.decode() for w in l.split(b'\x00'))
                   for l in subprocess.check_output(
                       ['find', '-H', str(batch), '-type', 'l', '-print0',
                        '-exec', 'readlink', '{}', ';']).strip().split(b'\n')])


def mkdir(path, parents=False, exist_ok=False):
    path = Path(path)
    if not parents or len(path.parts) == 1:
        if not (exist_ok and path.is_dir()):
            path.mkdir()
    else:
        os.makedirs(str(path), exist_ok=exist_ok)
    return path


def make_nonwritable(path):
    path = str(path)
    os.chmod(path, stat.S_IMODE(os.lstat(path).st_mode) & ~_writable)


def relink(path, linkname=None):
    link = Path(linkname) if linkname else Path(Path(path).name)
    if link.is_symlink():
        link.unlink()
    link.symlink_to(path)


def is_timestamp(s):
    return bool(re.match(r'^\d{4}-\d\d-\d\d_\d\d:\d\d:\d\d$', str(s)))


def filter_cmd(args):
    cmd = []
    for arg in args:
        if isinstance(arg, tuple):
            if arg[1]:
                cmd.extend(arg)
        elif isinstance(arg, list):
            cmd.extend(a for a in arg if a)
        elif arg:
            cmd.append(arg)
    return cmd


def find_program(cmd):
    return Path(subprocess.check_output(['which', cmd]).decode().strip()).resolve()


@contextmanager
def timing(name):
    if _dotiming:
        label = '>'.join(_timing_stack + [name])
        _timing[label]
        _timing_stack.append(name)
        tm = time.time()
    try:
        yield
    finally:
        if _dotiming:
            _timing[label] += time.time()-tm
            _timing_stack.pop(-1)


def print_timing():
    if _dotiming:
        groups = [sorted(group, key=lambda x: x[0])
                  for _, group
                  in groupby(_timing.items(), lambda x: x[0].split('>')[0])]
        groups.sort(key=lambda x: x[0][1], reverse=True)
        table = Table(align=['<', '<'])
        for group in groups:
            for row in group:
                table.add_row(re.sub(r'\w+>', 4*' ', row[0]),
                              '{:.4f}'.format(row[1]))
        print(table, file=sys.stderr)


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
    if isinstance(obj, (str, bytes)):
        return obj.split()
    elif isinstance(obj, tuple):
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


def groupby(lst, key):
    lst = [(key(x), x) for x in lst]
    lst.sort(key=lambda x: x[0])
    for k, group in itertools.groupby(lst, key=lambda x: x[0]):
        yield k, [x[1] for x in group]


class Configuration:
    def __init__(self, path):
        self.path = Path(path)
        self._dict = {}
        self.load()

    def __str__(self):
        return '\n'.join('{}\n\t{}'.format(name, val) for name, val in self._dict.items())

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, val):
        self._dict[key] = val

    def __contains__(self, x):
        return x in self._dict

    def get(self, key, default=None):
        return self._dict.get(key, default)

    def keys(self):
        return self._dict.keys()

    def load(self):
        if self.path.is_file():
            with self.path.open() as f:
                self._dict = yaml.load(f) or {}

    def save(self):
        if not self.path.parent.is_dir():
            mkdir(self.path.parent)
        with self.path.open('w') as f:
            yaml.dump(self._dict, f)
