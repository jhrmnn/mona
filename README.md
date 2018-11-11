# Mona

[![build](https://img.shields.io/travis/azag0/mona/master.svg)](https://travis-ci.org/azag0/mona)
[![coverage](https://img.shields.io/codecov/c/github/azag0/mona.svg)](https://codecov.io/gh/azag0/mona)
![python](https://img.shields.io/pypi/pyversions/mona.svg)
[![pypi](https://img.shields.io/pypi/v/mona.svg)](https://pypi.org/project/mona/)
[![commits since](https://img.shields.io/github/commits-since/azag0/mona/latest.svg)](https://github.com/azag0/mona/releases)
[![last commit](https://img.shields.io/github/last-commit/azag0/mona.svg)](https://github.com/azag0/mona/commits/master)
[![license](https://img.shields.io/github/license/azag0/mona.svg)](https://github.com/azag0/mona/blob/master/LICENSE)
[![code style](https://img.shields.io/badge/code%20style-black-202020.svg)](https://github.com/ambv/black)

Mona is a calculation framework that provides [persistent](https://en.wikipedia.org/wiki/Persistence_(computer_science)) [memoization](https://en.wikipedia.org/wiki/Memoization) and turns the Python call stack into a task [dependency graph](https://en.wikipedia.org/wiki/Dependency_graph).

## Installing

Install and update using [Pip](https://pip.pypa.io/en/stable/quickstart/).

```
pip install -U mona
```

## A simple example

```python
from mona import Mona, Rule

app = Mona()

@Rule
async def total(xs):
    return sum(xs)

@app.entry('fib', int)
@Rule
async def fib(n):
    if n <= 2:
        return 1
    return total([fib(n - 1), fib(n - 2)])
```

```
$ export MONA_APP=fib:app
$ mona init
Initializing an empty repository in /home/mona/fib/.mona.
$ mona run fib 5
7c3947: fib(5): will run
0383f6: fib(3): will run
b0287d: fib(4): will run
f47d51: fib(1): will run
9fd61c: fib(2): will run
45c92d: total([fib(2), fib(1)]): will run
2c136c: total([fib(3), fib(2)]): will run
521a8b: total([fib(4), fib(3)]): will run
Finished
$ mona graph
```

<img src="https://raw.githubusercontent.com/azag0/mona/master/docs/fib.svg?sanitize=true" alt width="350">

```python
from fib import app, fib

with app.create_session() as sess:
    assert sess.eval(fib(5)) == sum(sess.eval([fib(4), fib(3)]))
```

## Links

- Documentation: https://azag0.github.io/mona

