# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import json

import pytest  # type: ignore

import caf2 as caf
from caf2.files import dir_task
from caf2.json_utils import ClassJSONDecoder, ClassJSONEncoder

debug_level = os.environ.get('CAF_DEBUG')
if debug_level:
    import logging
    logging.basicConfig()
    logging.getLogger('caf2').setLevel(int(debug_level))


def test_pass_through():
    with caf.Session() as sess:
        assert sess.eval(10) == 10


@caf.rule
def add(x, y):
    return x + y


@caf.rule
def fib(n):
    if n < 2:
        return n
    return add(fib(n-1), fib(n-2))


def test_no_session():
    with pytest.raises(caf.sessions.NoActiveSession):
        fib(10)


def test_arg_not_in_session():
    with pytest.raises(caf.sessions.ArgNotInSession):
        with caf.Session():
            task = fib(1)
        with caf.Session():
            fib(task[0])


def test_fut_not_in_session():
    with pytest.raises(caf.sessions.ArgNotInSession):
        with caf.Session():
            task = fib(1)
        with caf.Session():
            fib(task)


def test_fibonacci():
    with caf.Session() as sess:
        assert sess.eval(fib(10)) == 55


def test_fibonacci2():
    with caf.Session() as sess:
        sess.eval(fib(10))
        n_tasks = len(sess._tasks)
    with caf.Session() as sess:
        assert sess.eval([fib(5), fib(10)]) == [5, 55]
        assert n_tasks == len(sess._tasks)


def test_fibonacci3():
    @caf.rule
    def total(xs):
        return sum(xs)

    @caf.rule
    def fib(n):
        if n < 2:
            return n
        return total([fib(n-1), fib(n-2)])

    with caf.Session() as sess:
        assert sess.eval(fib(10)) == 55


def test_fibonacci4():
    @caf.rule
    def fib(n):
        if n < 2:
            return [[n]]
        return [[add(fib(n-1)[0][0], fib(n-2)[0][0])]]

    with caf.Session() as sess:
        assert sess.eval(fib(10)[0][0]) == 55


def test_recursion():
    @caf.rule
    def recurse(i):
        if i < 5:
            return recurse(i+1)
        return i

    with caf.Session() as sess:
        assert sess.eval(recurse(0)) == 5


def test_tasks_not_run():
    with caf.Session() as sess:
        fib(10)
        sess.eval(fib(1))
        assert len(sess._tasks) == 2


@caf.rule
def calcs():
    return [(
        dist,
        dir_task(
            '#!/bin/bash\nexpr $(cat input) "*" 2; true'.encode(),
            {'input': str(dist).encode()},
            label=str(dist)
        ).get('STDOUT', b'0')
    ) for dist in range(0, 5)]


@caf.rule
def analysis(results):
    return sum(int(res) for _, res in results)


def test_calc():
    with caf.Session() as sess:
        assert sess.eval(analysis(calcs())) == 20


@pytest.fixture
def db(tmpdir):
    conn = caf.cache.init_db(tmpdir.join('test.db'))
    yield conn
    conn.close()


def test_db(db):
    sess = caf.CachedSession(db)
    with sess:
        assert sess.eval(analysis(calcs())) == 20
    with sess:
        sess.eval(analysis(calcs()))
        assert len(sess._tasks) == 2


def test_partial_eval():
    with caf.Session() as sess:
        sess.run_task(calcs())
        sess.run_task(calcs().children[3])
        assert sess.run_task(analysis(calcs()), check_ready=False) == 6


def test_json_utils():
    class MyClass:
        def __init__(self, x):
            self.x = x

        def __hash__(self):
            return hash(self.x)

        def __eq__(self, other):
            return self.x == other.x

    obj = {'x': MyClass(1), 'ys': [MyClass(2)]}
    tape = set()
    jsonstr = json.dumps(
        obj,
        tape=tape,
        classes={MyClass: lambda x: {'x': x.x}},
        cls=ClassJSONEncoder
    )
    assert len(tape) == 2
    obj2 = json.loads(
        jsonstr,
        classes={MyClass: lambda dct: MyClass(dct['x'])},
        cls=ClassJSONDecoder
    )
    assert obj == obj2
