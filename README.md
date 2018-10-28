# Mona

[![build](https://img.shields.io/travis/azag0/mona/master.svg)](https://travis-ci.org/azag0/mona)
[![coverage](https://img.shields.io/codecov/c/github/azag0/mona.svg)](https://codecov.io/gh/azag0/mona)
![python](https://img.shields.io/pypi/pyversions/mona.svg)
[![pypi](https://img.shields.io/pypi/v/mona.svg)](https://pypi.org/project/mona/)
[![commits since](https://img.shields.io/github/commits-since/azag0/mona/latest.svg)](https://github.com/azag0/mona/releases)
[![last commit](https://img.shields.io/github/last-commit/azag0/mona.svg)](https://github.com/azag0/mona/commits/master)
[![license](https://img.shields.io/github/license/azag0/mona.svg)](https://github.com/azag0/mona/blob/master/LICENSE)
[![code style](https://img.shields.io/badge/code%20style-black-202020.svg)](https://github.com/ambv/black)

Mona is a distributed calculation framework that turns normal execution of Python functions into a graph of tasks. Each task is hashed by the code of its function and its inputs, and the result of each executed task is cached. The cache can be stored persistently in an SQLite database. Tasks and their results can be exchanged between different machines via SSH.

## Installing

Install and update using [Pip](https://pip.pypa.io/en/stable/quickstart/).

```
pip install -U mona
```

## Links

- Documentation: https://azag0.github.io/mona

## A simple example

```python
from mona import Rule, Session

@Rule
async def total(xs):
    return sum(xs)

@Rule
async def fib(n):
    if n < 2:
        return n
    return total([fib(n - 1), fib(n - 2)])

with Session() as sess:
    sess.eval(fib(5))
    dot = sess.dot_graph()
dot.render(view=True)
```

![](https://raw.githubusercontent.com/azag0/mona/master/docs/fib.gv.svg?sanitize=true)

