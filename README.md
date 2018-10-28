# Caf

[![build](https://img.shields.io/travis/azag0/calcfw/master.svg)](https://travis-ci.org/azag0/calcfw)
[![coverage](https://img.shields.io/codecov/c/github/azag0/calcfw.svg)](https://codecov.io/gh/azag0/calcfw)
![python](https://img.shields.io/pypi/pyversions/calcfw.svg)
[![pypi](https://img.shields.io/pypi/v/calcfw.svg)](https://pypi.org/project/calcfw/)
[![commits since](https://img.shields.io/github/commits-since/azag0/calcfw/latest.svg)](https://github.com/azag0/calcfw/releases)
[![last commit](https://img.shields.io/github/last-commit/azag0/calcfw.svg)](https://github.com/azag0/calcfw/commits/master)
[![license](https://img.shields.io/github/license/azag0/calcfw.svg)](https://github.com/azag0/calcfw/blob/master/LICENSE)
[![code style](https://img.shields.io/badge/code%20style-black-202020.svg)](https://github.com/ambv/black)

Caf is a distributed calculation framework that turns normal execution of Python functions into a graph of tasks. Each task is hashed by the code of its function and its inputs, and the result of each executed task is cached. The cache can be stored persistently in an SQLite database. Tasks and their results can be exchanged between different machines via SSH.

## Installing

Install and update using [Pip](https://pip.pypa.io/en/stable/quickstart/).

```
pip install -U calcfw
```

## Links

- Documentation: https://azag0.github.io/calcfw

## A simple example

```python
import caf

@caf.Rule
async def total(xs):
    return sum(xs)

@caf.Rule
async def fib(n):
    if n < 2:
        return n
    return total([fib(n - 1), fib(n - 2)])

with caf.Session() as sess:
    sess.eval(fib(5))
    dot = sess.dot_graph()
dot.render('fib.gv', view=True, format='svg')
```

![](https://raw.githubusercontent.com/azag0/calcfw/master/docs/fib.gv.svg?sanitize=true)

