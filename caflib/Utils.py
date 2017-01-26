import re
import os
from contextlib import contextmanager
from datetime import datetime
from itertools import groupby as groupby_
import stat


def config_items(config, group=None):
    if not group:
        yield from config.items()
    else:
        for name, section in config.items():
            m = re.match(r'(?P<group>\w+) *"(?P<member>\w+)"', name)
            if m and m['group'] == group:
                yield m['member'], section


def slugify(s, path=False):
    s = re.sub(r'[^:_0-9a-zA-Z.()=+#/]', '-', s)
    if not path:
        s = s.replace('/', '-')
    return s


def get_timestamp():
    return format(datetime.today(), '%Y-%m-%d_%H:%M:%S')


def make_nonwritable(path):
    os.chmod(
        path,
        stat.S_IMODE(os.lstat(path).st_mode) &
        ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    )


def filter_cmd(args):
    cmd = []
    for arg in args:
        if isinstance(arg, tuple):
            if arg[1] is not None:
                if isinstance(arg[1], bool):
                    if arg[1]:
                        cmd.append(str(arg[0]))
                else:
                    cmd.extend(map(str, arg))
        elif isinstance(arg, list):
            cmd.extend(str(a) for a in arg if a)
        elif arg:
            cmd.append(str(arg))
    return cmd


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


def groupby(lst, key):
    lst = [(key(x), x) for x in lst]
    lst.sort(key=lambda x: x[0])
    for k, group in groupby_(lst, key=lambda x: x[0]):
        yield k, [x[1] for x in group]
