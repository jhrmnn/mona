import pytest  # type: ignore

from caf2 import Rule, Session


@Rule
def identity(x):
    return x


@Rule
def total(xs):
    return sum(xs)


def test_pass_through():
    with Session() as sess:
        assert sess.eval(10) == 10


def test_returned_done_future():
    @Rule
    def f(x):
        if x < 0:
            return x
        return f(-x)

    with Session() as sess:
        sess.eval(f(-4))
        assert sess.eval(f(4)) == -4


def test_identical_futures():
    @Rule
    def f(x, y):
        x, y = x[0], y[0]
        m = min(x, y)
        if m < 0:
            return [0]
        return [f([m], [max(x, y)-1])[0]]

    with Session() as sess:
        assert sess.eval(f([f([1], [1])[0]], [f([1], [1])[0]])[0]) == 0


def test_recursion():
    @Rule
    def recurse(i):
        if i < 5:
            return recurse(i+1)
        return i

    with Session() as sess:
        assert sess.eval(recurse(0)) == 5


def test_tasks_not_run():
    with pytest.warns(RuntimeWarning):
        with Session() as sess:
            identity(10)
            sess.eval(identity(1))
            assert len(sess._tasks) == 2


@pytest.mark.filterwarnings("ignore:tasks were never run")
def test_partial_eval():
    @Rule
    def multi():
        return [identity(x, default=0) for x in range(5)]

    with Session() as sess:
        sess.run_task(multi())
        tasks = multi().future_result().resolve()
        assert tasks[3] == multi().side_effects[3]
        sess.run_task(tasks[3])
        assert total(multi()).call() == 3
