# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import re
import os
from contextlib import contextmanager
from datetime import datetime
from itertools import groupby as groupby_
import stat
import random

from typing import (
    Dict, Any, Generator, Tuple, Iterable, TypeVar, List, Callable
)

_T = TypeVar('_T')
_V = TypeVar('_V')


def config_items(config: Dict[str, Any], group: str = None) \
        -> Generator[Tuple[str, Any], None, None]:
    if not group:
        yield from config.items()
    else:
        for name, section in config.items():
            m = re.match(r'(?P<group>\w+) *"(?P<member>\w+)"', name)
            if m and m['group'] == group:
                yield m['member'], section


def slugify(s: str, path: bool = False) -> str:
    s = re.sub(r'[^:_0-9a-zA-Z.()=+#/]', '-', s)
    if not path:
        s = s.replace('/', '-')
    return s


def get_timestamp() -> str:
    return format(datetime.today(), '%Y-%m-%d_%H:%M:%S')


def make_nonwritable(path: os.PathLike) -> None:
    os.chmod(
        path,
        stat.S_IMODE(os.lstat(path).st_mode) &
        ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    )


def sample(seq: Iterable[_T]) -> Generator[_T, None, None]:
    queue = list(seq)
    while queue:
        yield queue.pop(random.randrange(0, len(queue)))


def filter_cmd(args: List[Any]) -> List[Any]:
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
def cd(path: str) -> Generator[None, None, None]:
    path = str(path)
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


def listify(obj: Any) -> List[Any]:
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


def groupby(lst: Iterable[_T], key: Callable[[_T], _V]) \
        -> Generator[Tuple[_V, List[_T]], None, None]:
    keylst = [(key(x), x) for x in lst]
    keylst.sort(key=lambda x: x[0])
    for k, group in groupby_(keylst, key=lambda x: x[0]):
        yield k, [x[1] for x in group]
