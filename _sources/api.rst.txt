API
===

Rules and Sessions
------------------

.. module:: mona

.. autoclass:: Rule
    :members:
    :inherited-members:

.. autofunction:: labelled

.. autofunction:: run_shell

.. autofunction:: run_process

.. autofunction:: run_thread

.. autoclass:: Session
    :members:


.. module:: mona.rules

Directory tasks
---------------

.. autofunction:: dir_task

.. autoclass:: DirtaskTmpdir
    :members:

Scientific calculations
-----------------------

Molecular and crystal geometries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. module:: mona.sci.geomlib

.. autoclass:: Atom
    :members:

.. autoclass:: Molecule
    :members:

.. autoclass:: Crystal
    :members:

.. autofunction:: readfile

.. autofunction:: load

.. autofunction:: loads

FHI-aims
^^^^^^^^

.. module:: mona.sci.aims

.. autoclass:: Aims
    :members:

.. autoclass:: SpeciesDefaults
    :members:

Futures and Tasks
-----------------

The following classes are not intended for direct use by the end user, but are
part of the versioned public API neverthless.

.. autoclass:: mona.futures.Future
    :members:

.. autoclass:: mona.hashing.HashedComposite
    :members:
    :inherited-members:

.. autoclass:: mona.tasks.Task
    :members:
    :inherited-members:

.. autoclass:: mona.tasks.TaskComposite
    :members:
    :inherited-members:

.. autoclass:: mona.tasks.TaskComponent
    :members:
    :inherited-members:
