import subprocess

import pytest  # type: ignore

from caf import Rule, Session, run_thread, run_shell
from caf.rules import with_hook


@Rule
async def identity(x):
    return x


@Rule
async def total(xs):
    return sum(xs)


@Rule
async def multi(n):
    return [identity(x, default=0) for x in range(n)]


def test_pass_through():
    with Session() as sess:
        assert sess.eval(10) == 10


def test_object():
    @Rule
    async def get_object():
        return object()

    with Session() as sess:
        sess.eval(get_object())


def test_returned_done_future():
    @Rule
    async def f(x):
        if x < 0:
            return x
        return f(-x)

    with Session() as sess:
        sess.eval(f(-4))
        assert sess.eval(f(4)) == -4


def test_identical_futures():
    @Rule
    async def f(x, y):
        x, y = x[0], y[0]
        m = min(x, y)
        if m < 0:
            return [0]
        return [f([m], [max(x, y) - 1])[0]]

    with Session() as sess:
        expr = f([f([1], [1])[0]], [f([1], [1])[0]])[0]
        assert sess.eval(expr, depth=True) == 0


def test_recursion():
    @Rule
    async def recurse(i):
        if i < 5:
            return recurse(i + 1)
        return i

    with Session() as sess:
        assert sess.eval(recurse(0)) == 5


def test_tasks_not_run():
    with pytest.warns(RuntimeWarning):
        with Session() as sess:
            identity(10)
            sess.eval(identity(1))
            assert len(sess._tasks) == 2


@pytest.mark.filterwarnings('ignore:tasks have never run')
def test_partial_eval():
    with Session() as sess:
        main = multi(5)
        sess.run_task(main)
        tasks = main.future_result().resolve()
        sess.run_task(tasks[3])
        assert total(main).call() == 3


def test_graphviz():
    with Session() as sess:
        sess.eval(identity(multi(5)))
        dot = sess.dot_graph(format='svg')
        assert len(dot.source.split('\n')) == 20


def test_with_hook():
    @with_hook('test')
    @Rule
    async def f(x):
        return 1

    with Session() as sess:
        sess.storage['hook:test'] = lambda x: x
        task = f(1)
        sess.eval(task)


def test_local_storage():
    @Rule
    async def f():
        return Session.active().running_task.storage['test']

    with Session() as sess:
        f().storage['test'] = 3
        assert sess.run_task(f()).value == 3


def test_run_thread():
    @Rule
    async def f():
        return await run_thread(lambda: 1)

    with Session() as sess:
        assert sess.run_task(f()).value == 1


def test_stderr():
    @Rule
    async def f():
        return await run_shell('echo 5 1>&2', stderr=subprocess.PIPE)

    with Session() as sess:
        assert int(sess.eval(f()[1])) == 5
