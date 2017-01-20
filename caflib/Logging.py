import sys
from io import StringIO
from itertools import chain
from datetime import datetime
from pathlib import Path


def log_caf(argv):
    if not Path('.caf').is_dir():
        Path('.caf').mkdir()
    with open('.caf/log', 'a') as f:
        f.write('{:%Y-%b-%d %H:%M:%S}: {}\n'.format(datetime.now(), ' '.join(argv)))


class colstr(str):
    colors = {'bold': '\x1b[01;1m',
              'red': '\x1b[01;31m',
              'green': '\x1b[32m',
              'yellow': '\x1b[33m',
              'pink': '\x1b[35m',
              'blue': '\x1b[01;34m',
              'cyan': '\x1b[36m',
              'grey': '\x1b[37m',
              'normal': '\x1b[0m'}

    def __new__(cls, s, color):
        obj = str.__new__(cls, colstr.colors[color] + str(s) + colstr.colors['normal'])
        obj.len = len(str(s))
        obj.orig = str(s)
        return obj

    def __len__(self):
        return self.len


def warn(s):
    print(
        colstr(s, 'yellow'),
        file=sys.stdout if sys.stdout.isatty() else sys.stderr
    )


def info(s):
    print(
        colstr(s, 'green'),
        file=sys.stdout if sys.stdout.isatty() else sys.stderr
    )


def error(s):
    print(
        colstr(s, 'red'),
        file=sys.stdout if sys.stdout.isatty() else sys.stderr
    )


def dep_error(dep):
    print(colstr('caf requires {}'.format(dep), 'red'))
    print('Install all dependencies with\n\n'
          '    pip3 install docopt pyyaml progressbar2')
    sys.exit(1)


class TableException(Exception):
    pass


def alignize(s, align, width):
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
    def __init__(self, **kwargs):
        self.rows = []
        self.set_format(**kwargs)

    def add_row(self, *row, free=False):
        self.rows.append((free, row))

    def set_format(self, sep=' ', align='>', indent=''):
        self.sep = sep
        self.align = align
        self.indent = indent

    def sort(self, key=lambda x: x[0]):
        self.rows.sort(key=lambda x: key(x[1]), reverse=True)

    def __str__(self):
        col_nums = [len(row) for free, row in self.rows if not free]
        if len(set(col_nums)) != 1:
            raise TableException('Unequal column lengths: {}'.format(col_nums))
        col_nums = col_nums[0]
        cell_widths = [[len(cell) for cell in row]
                       for free, row in self.rows if not free]
        col_widths = [max(col) for col in zip(*cell_widths)]
        seps = (col_nums-1)*[self.sep] if not isinstance(self.sep, list) else self.sep
        seps += ['\n']
        aligns = col_nums*[self.align] if not isinstance(self.align, list) else self.align
        f = StringIO()
        for free, row in self.rows:
            if free:
                f.write('{}\n'.format(row[0]))
            else:
                cells = (alignize(cell, align, width)
                         for cell, align, width
                         in zip(row, aligns, col_widths))
                f.write('{}{}'.format(self.indent,
                                      ''.join(chain.from_iterable(zip(cells, seps)))))
        return f.getvalue()[:-1]
