API
===

Rules and sessions
------------------

.. automodule:: mona
    :members:

Session plugins
---------------

.. automodule:: mona.plugins
    :members:

Files
-----

.. automodule:: mona.files
    :members:

Directory tasks
---------------

.. automodule:: mona.dirtask
    :members:

.. autofunction:: dir_task

Scientific calculations
-----------------------

Molecular and crystal geometries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: mona.sci.geomlib
    :members:

FHI-aims
^^^^^^^^

.. automodule:: mona.sci.aims
    :members:

.. autofunction:: parse_aims

LaTeX
^^^^^

.. automodule:: mona.sci.tex
    :members:

Futures and tasks
-----------------

The following classes are not intended for direct use by the end user, but are
part of the versioned public API neverthless.

.. autoclass:: mona.futures.Future

.. autoclass:: mona.hashing.HashedComposite

.. autoclass:: mona.tasks.Task

.. autoclass:: mona.tasks.TaskComposite

.. autoclass:: mona.tasks.TaskComponent
