from caf import Rule, Session

from tests.test_core import total


@Rule
async def add(x, y):
    return x + y


@Rule
async def fib(n):
    if n < 2:
        return n
    return add(fib(n-1), fib(n-2))


def test_fibonacci():
    with Session() as sess:
        assert sess.eval(fib(10)) == 55


def test_fibonacci2():
    with Session() as sess:
        sess.eval(fib(10))
        n_tasks = len(sess._tasks)
    with Session() as sess:
        assert sess.eval([fib(5), fib(10)]) == [5, 55]
        assert n_tasks == len(sess._tasks)


def test_fibonacci3():
    @Rule
    async def fib(n):
        if n < 2:
            return n
        return total([fib(n-1), fib(n-2)])

    with Session() as sess:
        assert sess.eval(fib(10)) == 55


def test_fibonacci4():
    @Rule
    async def fib(n):
        if n < 2:
            return [[n]]
        return [[add(fib(n-1)[0][0], fib(n-2)[0][0])]]

    with Session() as sess:
        assert sess.eval(fib(10)[0][0]) == 55
