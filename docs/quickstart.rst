Quickstart
==========

Fibonacci numbers
-----------------

A Fibonacci number :math:`F_n` is defined recursively as :math:`F_n=F_{n-1}+F_{n-2}`, with :math:`F_1=F_2=1`. Because the definition contains references to two Fibonacci numbers, a naive recursive implementation of :math:`F_n` has a factorial time complexity. With Mona, the implementation could look something like this::

    import mona

    @mona.Rule
    async def total(xs):
        return sum(xs)

    @mona.Rule
    async def fib(n):
        if n <= 2:
            return 1
        return total([fib(n - 1), fib(n - 2)])

One thing to note about ``fib()`` and ``total()`` is that although they are defined as coroutine functions, they are called without ``await``. Another is that if they were defined as ordinary (non-async) functions and were not decorated, ``fib(n)`` would evaluate directly to :math:`F_n`, albeit with factorial time complexity.

As it stands above, though, each of the two decorated functions becomes a :class:`~mona.Rule`. Calling a rule does not execute the body of the coroutine function, nor does it create a coroutine, as coroutine functions do. Rather, calling it creates a :class:`~mona.tasks.Task`. Furthermore, a task can be created only in a so-called session, represented by the :class:`~mona.Session` context manager. The session is also used to actually execute the function associated with a task and to get its result---to evaluate the task::

    with mona.Session() as sess:
        assert sess.eval(fib(35)) == 9227465

One result of using Mona is that here, the Fibonacci number was calculated with linear time complexity, even though the implementation looks recursive. This is achieved by caching the results of all tasks.

Alternatively, one can evaluate the task with all the bells and whistles using the CLI::

    $ mona init
    Initializing an empty repository in /home/mona/fib/.mona.
    $ ls
    .mona  fib.py
    $ mona run fib:fib 35
    1837a5: fib(35): will run
    7449e3: fib(33): will run
    248b29: fib(34): will run
    ...
    0694d9: total([fib(32), fib(31)]): will run
    5a29eb: total([fib(33), fib(32)]): will run
    b628ff: total([fib(34), fib(33)]): will run
    Finished
    $ mona run fib:fib 34
    Finished

Apart from the reduced time complexity, here the task cache was stored in an SQLite database at ``.mona/cache.db``, and used in the second invocation of the ``run`` command.

To use these cached results in Python, one uses a session created by an :class:`~mona.app.App` instance::

    from mona.app import App

    with App().session() as sess:
        sess.eval(fib(35))

What exactly happened underneath in the previous examples is explained in the next section.

Under the hood
--------------
