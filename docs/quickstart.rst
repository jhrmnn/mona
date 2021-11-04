Quickstart
==========

Fibonacci numbers
-----------------

A Fibonacci number :math:`F_n` is defined recursively as :math:`F_n=F_{n-1}+F_{n-2}`, with :math:`F_1=F_2=1`. A naive Python implementation is a four-liner::

    def fib(n):
        if n <= 2:
            return 1
        return fib(n - 1) + fib(n - 2)

The major issue of this approach is that to evaluate :math:`F_n`, :math:`F_{n-k}` must be called :math:`\sim2^k` times, leading to overall exponential time complexity in :math:`n`. A common remedy is to *memoize* ``fib()``---to make it remember its results for given arguments. This circumvents the exponentially branching recursion.

This takes only a little work with Mona::

    from mona import Rule

    @Rule
    def add(x, y):
        return x + y

    @Rule
    def fib(n):
        if n <= 2:
            return 1
        return add(fib(n - 1), fib(n - 2))

We have decorated ``fib()`` with :class:`~mona.Rule` and replaced ``x + y`` by ``add(x, y)``. Calling a rule then does not actually run the body of the function but creates a :class:`~mona.tasks.Task`.

The extra ``add()`` rule is needed, because tasks created by calling rules may be yet unevaluated, and their results inaccessible. But Mona ensures that a task (such as ``add(fib(2), fib(1))``) is run only when its inputs (``fib(2)`` and ``fib(1)``) have been evaluated, and passes in the evaluated arguments.

The rules cannot be called anytime, but only in an active :class:`~mona.Session`. The session tracks the tasks created by calling rules and provides means to evaluate the tasks. A bare session object memoizes rules only non-persistently and executes tasks sequentially, but is extensible by session plugins, which add all the extra functionality. Rather than working with sessions directly, this tutorial uses the :class:`~mona.Mona` application, which bootstraps a fully equipped session and provides additional utilities. The extra functionality includes persistent memoization, parallel execution, file handling, and handling of temporary directories for task execution.

To make a rule accessible to the command-line interface of Mona, one decorates it with :meth:`Mona.entry`::

    from mona import Mona, Rule

    app = Mona()

    @Rule
    def add(x, y):
        return x + y

    @app.entry('fib', int)
    @Rule
    def fib(n):
        if n <= 2:
            return 1
        return add(fib(n - 1), fib(n - 2))

Saving this file in ``fib.py``, one can then call the ``fib()`` rule from a terminal::

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

To use the cached results in Python, one uses a session created by the :class:`~mona.Mona` application::

    from fib import app, fib

    with app.create_session() as sess:
        assert sess.eval(fib(5)) == sum(sess.eval([fib(4), fib(3)]))

What exactly happened underneath in the previous examples is explained in the next section.

Under the hood
--------------
