# Caf — Distributed calculation framework

[![build](https://img.shields.io/travis/azag0/calcfw.svg)](https://travis-ci.org/azag0/calcfw)
[![coverage](https://img.shields.io/codecov/c/github/azag0/calcfw.svg)](https://codecov.io/gh/azag0/calcfw)
![python](https://img.shields.io/pypi/pyversions/calcfw.svg)
[![pypi](https://img.shields.io/pypi/v/calcfw.svg)](https://pypi.org/project/calcfw/)
[![last commit](https://img.shields.io/github/last-commit/azag0/calcfw.svg)](https://github.com/azag0/calcfw/commits/master)
[![license](https://img.shields.io/github/license/azag0/libmbd.svg)](https://github.com/azag0/libmbd/blob/master/LICENSE)

```python
import caf2 as caf

@caf.Rule
async def total(xs):
    return sum(xs)

@caf.Rule
async def fib(n):
    if n < 2:
        return n
    return total([fib(n-1), fib(n-2)])

with caf.Session() as sess:
    sess.eval(fib(5))
    dot = sess.dot_graph(format='svg')
dot.render('fib.gv', view=True)
```

![](https://raw.githubusercontent.com/azag0/calcfw/master/docs/fib.gv.svg?sanitize=true)
