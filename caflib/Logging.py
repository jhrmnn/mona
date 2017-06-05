# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sys
import os
from io import StringIO
from itertools import chain

from typing import Callable, Any, List, Tuple, TYPE_CHECKING  # noqa
if TYPE_CHECKING:
    from mypy_extensions import NoReturn
else:
    NoReturn = None


DEBUG = 'DEBUG' in os.environ


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

    def __new__(cls, s: str, color: str) -> str:
        return str.__new__(  # type: ignore
            cls,
            colstr.colors[color] + str(s) + colstr.colors['normal']
        )

    def __init__(self, s: str, color: str) -> None:
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


def error(s: str) -> NoReturn:
    print(
        colstr(s, 'red'),
        file=sys.stdout if sys.stdout.isatty() else sys.stderr
    )
    sys.exit(1)


def no_cafdir() -> None:
    error('Not a caf repository')


_reports = []


def report(f: Callable) -> Callable:
    """Register function as a report.

    Example:

        @report
        def my_report(...
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


class TableException(Exception):
    pass


def alignize(s: str, align: str, width: int) -> str:
    l = len(s)
    if l < width:
        if align == '<':
            s = s + (width-l)*' '
        elif align == '>':
            s = (width-l)*' ' + s
        elif align == '|':
            s = (-(l-width)//2)*' ' + s + ((width-l)//2)*' '
    return s


class Table:
    def __init__(self, **kwargs: Any) -> None:
        self.rows: List[Tuple[bool, Tuple[Any, ...]]] = []
        self.set_format(**kwargs)

    def add_row(self, *row: Any, free: bool = False) -> None:
        self.rows.append((free, row))

    def set_format(self, sep: str = ' ', align: str = '>', indent: str = '') -> None:
        self.sep = sep
        self.align = align
        self.indent = indent

    def sort(self, key: Callable[[Any], Any] = lambda x: x[0]) -> None:
        self.rows.sort(key=lambda x: key(x[1]), reverse=True)

    def __str__(self) -> str:
        col_nums = [len(row) for free, row in self.rows if not free]
        if len(set(col_nums)) != 1:
            raise TableException(f'Unequal column lengths: {col_nums}')
        col_num = col_nums[0]
        cell_widths = [[len(cell) for cell in row]
                       for free, row in self.rows if not free]
        col_widths = [max(col) for col in zip(*cell_widths)]
        seps = (col_num-1)*[self.sep] if not isinstance(self.sep, list) \
            else self.sep
        seps += ['\n']
        aligns = col_num*[self.align] if not isinstance(self.align, list) \
            else self.align
        f = StringIO()
        for free, row in self.rows:
            if free:
                f.write(f'{row[0]}\n')
            else:
                cells = (alignize(cell, align, width)
                         for cell, align, width
                         in zip(row, aligns, col_widths))
                f.write(
                    self.indent + ''.join(chain.from_iterable(zip(cells, seps)))
                )
        return f.getvalue()[:-1]
