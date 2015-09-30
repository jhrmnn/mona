import sys

_colors = {
    'BOLD': '\x1b[01;1m',
    'RED': '\x1b[01;31m',
    'GREEN': '\x1b[32m',
    'YELLOW': '\x1b[33m',
    'PINK': '\x1b[35m',
    'BLUE': '\x1b[01;34m',
    'CYAN': '\x1b[36m',
    'GREY': '\x1b[37m',
    'NORMAL': '\x1b[0m',
}


def warn(s):
    print(_colors['YELLOW'] + s + _colors['NORMAL'])


def info(s):
    print(_colors['GREEN'] + s + _colors['NORMAL'])


def error(s):
    print(_colors['RED'] + s + _colors['NORMAL'])
    sys.exit(1)
