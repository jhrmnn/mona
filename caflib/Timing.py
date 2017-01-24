import time
import os
import sys
import re
from contextlib import contextmanager
from collections import defaultdict


from caflib.Utils import groupby
from caflib.Logging import Table


class Timer:
    def __init__(self):
        self.active = 'TIMING' in os.environ
        self.timing = defaultdict(float)
        self.stack = []

    def __del__(self):
        if self.active:
            groups = [
                sorted(group, key=lambda x: x[0])
                for _, group
                in groupby(self.timing.items(), lambda x: x[0].split('>')[0])
            ]
            groups.sort(key=lambda x: x[0][1], reverse=True)
            table = Table(align=['<', '<'])
            for group in groups:
                for row in group:
                    table.add_row(re.sub(r'\w+>', 4*' ', row[0]), f'{row[1]:.4f}')
            print(table, file=sys.stderr)


_timer = Timer()


@contextmanager
def timing(name):
    if _timer.active:
        label = '>'.join(_timer.stack + [name])
        _timer.timing[label]
        _timer.stack.append(name)
        tm = time.time()
    try:
        yield
    finally:
        if _timer.active:
            _timer.timing[label] += time.time()-tm
            _timer.stack.pop(-1)
