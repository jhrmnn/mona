CLI
===

.. code-block:: none

    Usage: caf [OPTIONS] COMMAND [ARGS]...

    Options:
      --help  Show this message and exit.

    Commands:
      checkout  Checkout path-labeled tasks into a directory tree.
      graph     Open a pdf with the task graph.
      init      Initialize a Git repository.
      run       Run a given rule.
      status    Print status of tasks.


.. code-block:: none

    Usage: caf run [OPTIONS] RULE

      Run a given rule.

    Options:
      -p, --pattern TEXT   Tasks to be executed
      -P, --path           Execute path-like tasks
      -j, --cores INTEGER  Number of cores
      -l, --limit INTEGER  Limit number of tasks to N
      --maxerror INTEGER   Number of errors in row to quit
      --help               Show this message and exit.
