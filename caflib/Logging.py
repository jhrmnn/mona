import sys
from io import StringIO
from itertools import chain, dropwhile


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
    print(colstr(s, 'yellow'))


def info(s):
    print(colstr(s, 'green'))


def error(s):
    print(colstr(s, 'red'))
    sys.exit(1)


class TableException(Exception):
    pass


class Table:
    def __init__(self, **kwargs):
        self.rows = []
        self.set_format(**kwargs)

    def add_row(self, *row, free=False):
        self.rows.append((free, row))

    def set_format(self, sep=' ', align='>'):
        self.sep = sep
        self.align = align

    def sort(self, key=lambda x: x[0]):
        self.rows.sort(key=lambda x: key(x[1]), reverse=True)

    def __str__(self):
        col_nums = [len(row) for free, row in self.rows if not free]
        if len(set(col_nums)) != 1:
            raise TableException('Unequal column lengths: {}'.format(col_nums))
        col_nums = len(next(dropwhile(lambda r: r[0], self.rows))[1])
        cell_widths = [[len(str(cell)) for cell in row]
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
                cells = ('{:{align}{width}}'
                         .format(str(cell), align=align, width=width)
                         for cell, align, width
                         in zip(row, aligns, col_widths))
                f.write('{}'.format(''.join(chain.from_iterable(zip(cells, seps)))))
        return f.getvalue()[:-1]
