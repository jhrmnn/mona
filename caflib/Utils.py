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


def normalize_str(s):
    return re.sub(r'[^0-9a-zA-Z.()=+#]', '-', s)


def slugify(x, top=True):
    if isinstance(x, str):
        return normalize_str(x)
    if isinstance(x, bytes):
        return normalize_str(x.encode())
    if top:
        try:
            return '_'.join(_slugify(x) for x in x)
        except TypeError:
            pass
    if isinstance(x, tuple):
        return f'{normalize_str(str(x[0]))}={_slugify(x[1])}'
    else:
        try:
            return ':'.join(_slugify(x) for x in x)
        except TypeError:
            return normalize_str(str(x))


def _slugify(x):
    return slugify(x, top=False)


def get_timestamp():
    return format(datetime.today(), '%Y-%m-%d_%H:%M:%S')


# def mkdir(path, parents=False, exist_ok=False):
#     path = Path(path)
#     if not parents or len(path.parts) == 1:
#         if not (exist_ok and path.is_dir()):
#             path.mkdir()
#     else:
#         os.makedirs(str(path), exist_ok=exist_ok)
#     return path


def make_nonwritable(path):
    os.chmod(
        path,
        stat.S_IMODE(os.lstat(path).st_mode) &
        ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    )


# def relink(path, linkname=None, relative=True):
#     link = Path(linkname) if linkname else Path(Path(path).name)
#     if link.is_symlink():
#         link.unlink()
#     if not link.parent.is_dir():
#         mkdir(link.parent, parents=True)
#     if relative:
#         link.symlink_to(os.path.relpath(str(path), str(link.parent)))
#     else:
#         link.symlink_to(path)


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
