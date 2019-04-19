# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itertools import chain, starmap
from typing import Any, Callable, List, Tuple, Union

__all__ = ()


class lenstr(str):  # noqa: N801
    def __new__(cls, s: Any, len: int) -> str:
        return str.__new__(cls, s)  # type: ignore

    def __init__(self, s: Any, len: int) -> None:
        self._len = len

    def __len__(self) -> int:
        return self._len


def align(s: str, align: str, width: int) -> str:
    l = len(s)
    if l >= width:
        return s
    if align == '<':
        s = s + (width - l) * ' '
    elif align == '>':
        s = (width - l) * ' ' + s
    elif align == '|':
        s = (-(l - width) // 2) * ' ' + s + ((width - l) // 2) * ' '
    return s


class Table:
    def __init__(self, **kwargs: Any) -> None:
        self._rows: List[Tuple[bool, Tuple[str, ...]]] = []
        self.set_format(**kwargs)

    def add_row(self, *row: str, free: bool = False) -> None:
        self._rows.append((free, row))

    def set_format(
        self,
        sep: Union[str, List[str]] = ' ',
        align: Union[str, List[str]] = '>',
        indent: str = '',
    ) -> None:
        self._sep = sep
        self._align = align
        self._indent = indent

    def sort(
        self,
        key: Callable[[Tuple[object, ...]], object] = lambda x: x[0],
        **kwargs: Any,
    ) -> None:
        self._rows.sort(key=lambda x: key(x[1]), **kwargs)

    def __str__(self) -> str:
        col_nums = [len(row) for free, row in self._rows if not free]
        if len(set(col_nums)) != 1:
            raise ValueError(f'Unequal column lengths: {col_nums}')
        col_num = col_nums[0]
        cells_widths = [
            [len(cell) for cell in row] for free, row in self._rows if not free
        ]
        col_widths = [max(cws) for cws in zip(*cells_widths)]
        if isinstance(self._sep, list):
            seps = self._sep
        else:
            seps = (col_num - 1) * [self._sep]
        seps += ['']
        if isinstance(self._align, list):
            aligns = self._align
        else:
            aligns = col_num * [self._align]
        lines: List[str] = []
        for free, row in self._rows:
            if free:
                lines += row[0]
            else:
                cells = starmap(align, zip(row, aligns, col_widths))
                lines.append(
                    self._indent
                    + ''.join(chain.from_iterable(zip(cells, seps))).rstrip()
                )
        return '\n'.join(lines)
