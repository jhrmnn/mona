API
===

.. module:: caf

.. autoclass:: Rule
    :members:
    :inherited-members:

.. autofunction:: labelled

.. autofunction:: run_shell

.. autofunction:: run_process

.. autofunction:: run_thread

.. autoclass:: Session
    :members:
    :inherited-members:

Futures and Tasks
-----------------

The following classes are not intended for direct use by the end user, but are
part of the versioned public API neverthless.

.. autoclass:: caf.futures.Future
    :members:

.. autoclass:: caf.hashing.HashedComposite
    :members:
    :inherited-members:

.. autoclass:: caf.tasks.Task
    :members:
    :inherited-members:

.. autoclass:: caf.tasks.TaskComposite
    :members:
    :inherited-members:

.. autoclass:: caf.tasks.TaskComponent
    :members:
    :inherited-members:
