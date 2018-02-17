# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sys
import os
from itertools import chain, starmap

from typing import Callable, Any, List, Tuple, Union
from mypy_extensions import NoReturn


DEBUG = bool(os.environ.get('DEBUG'))


class colstr(str):
    colors = {
        'red': '\x1b[31m',
        'green': '\x1b[32m',
        'yellow': '\x1b[33m',
        'blue': '\x1b[34m',
        'bryellow': '\x1b[93m',
        'brblue': '\x1b[94m',
        'pink': '\x1b[35m',
        'cyan': '\x1b[36m',
        'grey': '\x1b[37m',
        'normal': '\x1b[0m'
    }

    def __new__(cls, s: Any, color: str) -> str:
        return str.__new__(  # type: ignore
            cls,
            colstr.colors[color] + str(s) + colstr.colors['normal']
        )

    def __init__(self, s: Any, color: str) -> None:
        self.len = len(str(s))
        self.orig = str(s)

    def __len__(self) -> int:
        return self.len


def warn(s: str) -> None:
    print(
        colstr(s, 'yellow'),
        file=sys.stdout if sys.stdout.isatty() else sys.stderr
    )


def debug(s: str) -> None:
    if DEBUG:
        print(s)


def info(s: str) -> None:
    print(
        colstr(s, 'green'),
        file=sys.stdout if sys.stdout.isatty() else sys.stderr
    )


class CafError(Exception):
    pass


def error(s: str = None) -> NoReturn:
    raise CafError(s)


def print_error(s: str) -> None:
    if not s:
        return
    print(
        colstr(s, 'red'),
        file=sys.stdout if sys.stdout.isatty() else sys.stderr
    )


def no_cafdir() -> None:
    error('Not a caf repository')


_reports = []


def report(f: Callable[..., Any]) -> Callable[..., Any]:
    """Register function as a report.

    Example:

        @report
        def my_report(): ...
    """
    _reports.append(f)
    return f


def handle_broken_pipe() -> None:
    try:
        sys.stdout.flush()
    finally:
        try:
            sys.stdout.close()
        finally:
            try:
                sys.stderr.flush()
            finally:
                sys.stderr.close()


def alignize(s: str, align: str, width: int) -> str:
    l = len(s)
    if l >= width:
        return s
    if align == '<':
        s = s + (width-l)*' '
    elif align == '>':
        s = (width-l)*' ' + s
    elif align == '|':
        s = (-(l-width)//2)*' ' + s + ((width-l)//2)*' '
    return s


class Table:
    def __init__(self, **kwargs: Any) -> None:
        self.rows: List[Tuple[bool, Tuple[str, ...]]] = []
        self.set_format(**kwargs)

    def add_row(self, *row: str, free: bool = False) -> None:
        self.rows.append((free, row))

    def set_format(self,
                   sep: Union[str, List[str]]= ' ',
                   align: Union[str, List[str]] = '>',
                   indent: str = '') -> None:
        self.sep = sep
        self.align = align
        self.indent = indent

    def sort(self, key: Callable[[Tuple[Any, ...]], Any] = lambda x: x[0], **kwargs: Any) -> None:
        self.rows.sort(key=lambda x: key(x[1]), **kwargs)

    def __str__(self) -> str:
        col_nums = [len(row) for free, row in self.rows if not free]
        if len(set(col_nums)) != 1:
            raise ValueError(f'Unequal column lengths: {col_nums}')
        col_num = col_nums[0]
        cells_widths = [
            [len(cell) for cell in row] for free, row in self.rows if not free
        ]
        col_widths = [max(cws) for cws in zip(*cells_widths)]
        if isinstance(self.sep, list):
            seps = self.sep
        else:
            seps = (col_num-1)*[self.sep]
        seps += ['\n']
        if isinstance(self.align, list):
            aligns = self.align
        else:
            aligns = col_num*[self.align]
        s = ''
        for free, row in self.rows:
            if free:
                s += f'{row[0]}\n'
            else:
                cells = starmap(alignize, zip(row, aligns, col_widths))
                s += self.indent + ''.join(chain.from_iterable(zip(cells, seps)))
        return s
