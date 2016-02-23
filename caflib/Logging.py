import sys

_colors = {
    'bold': '\x1b[01;1m',
    'red': '\x1b[01;31m',
    'green': '\x1b[32m',
    'yellow': '\x1b[33m',
    'pink': '\x1b[35m',
    'blue': '\x1b[01;34m',
    'cyan': '\x1b[36m',
    'grey': '\x1b[37m',
    'normal': '\x1b[0m',
}


class Colored:
    def __init__(self, x):
        self.x = x

    def __format__(self, color):
        return _colors[color.lower()] + str(self.x) + _colors['normal']

    def __str__(self):
        return str(self.x)


def warn(s):
    print(_colors['yellow'] + s + _colors['normal'])


def info(s):
    print(_colors['green'] + s + _colors['normal'])


def error(s):
    print(_colors['red'] + s + _colors['normal'])
    sys.exit(1)
