Quickstart
==========

Fibonacci numbers
-----------------

A Fibonacci number :math:`F_n` is defined recursively as :math:`F_n=F_{n-1}+F_{n-2}`, with :math:`F_1=F_2=1`. A naive Python implementation is a four-liner::

    def fib(n):
        if n <= 2:
            return 1
        return fib(n - 1) + fib(n - 2)

The major issue of this approach is that to evaluate :math:`F_n`, :math:`F_{n-k}` must be called :math:`\sim2^k` times, leading to overall exponential time complexity in :math:`n`. A common remedy is to *memoize* ``fib()``---to make it remember its results for a given argument. This circumvents the exponentially branching recursion.

This takes only a little work with Mona, and as an extra bonus, the memoization will be persistent---``fib()`` will remember its results between different invocations of the Python interpreter::

    from mona import Mona, Rule

    app = Mona()

    @app.entry('fib', int)
    @Rule
    async def fib(n):
        if n <= 2:
            return 1
        return total([fib(n - 1), fib(n - 2)])

    @Rule
    async def total(xs):
        return sum(xs)


One thing to note about ``fib()`` and ``total()`` is that although they are defined as coroutine functions, they are called without ``await``. Another is that if they were defined as ordinary (non-async) functions and were not decorated, ``fib(n)`` would evaluate directly to :math:`F_n`.

As it stands above, though, each of the two decorated functions becomes a :class:`~mona.Rule`. Calling a rule does not execute the body of the coroutine function, nor does it create a coroutine, as coroutine functions do. Rather, calling it creates a :class:`~mona.tasks.Task`.

The ::

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

To use these cached results in Python, one uses a session created by the :class:`~mona.Mona` application::

    from fib import app, fib

    with app.create_session() as sess:
        assert sess.eval(fib(5)) == sum(sess.eval([fib(4), fib(3)]))

What exactly happened underneath in the previous examples is explained in the next section.

Under the hood
--------------
