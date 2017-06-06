# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import time
import os
import sys
import re
from contextlib import contextmanager
from collections import defaultdict

from caflib.Utils import groupby
from caflib.Logging import Table

from typing import DefaultDict, List, Iterator # noqa


class Timer:
    def __init__(self) -> None:
        self.active = 'TIMING' in os.environ
        self.timing: DefaultDict[str, float] = defaultdict(float)
        self.stack: List[str] = []

    def __del__(self) -> None:
        if self.active:
            groups = [
                sorted(group, key=lambda x: x[0])
                for _, group
                in groupby(self.timing.items(), lambda x: x[0].split('>')[0])
            ]
            groups.sort(key=lambda x: x[0][1], reverse=True)
            table = Table(align=['<', '<'])
            total = 0.
            for group in groups:
                for row in group:
                    table.add_row(re.sub(r'[^>]*>', 4*' ', row[0]), f'{row[1]:.4f}')
                    if '>' not in row[0]:
                        total += row[1]
            table.add_row('TOTAL', f'{total:.4f}')
            print(table, file=sys.stderr)


TIMER = Timer()


@contextmanager
def timing(name: str) -> Iterator[None]:
    if TIMER.active:
        label = '>'.join(TIMER.stack + [name])
        TIMER.timing[label]
        TIMER.stack.append(name)
        tm = time.time()
    try:
        yield
    finally:
        if TIMER.active:
            TIMER.timing[label] += time.time()-tm
            TIMER.stack.pop(-1)
