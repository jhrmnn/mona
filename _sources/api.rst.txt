API
===

Rules and Sessions
------------------

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


.. module:: caf.rules

Directory tasks
---------------

.. autofunction:: dir_task

.. autoclass:: DirtaskTmpdir
    :members:

Scientific calculations
-----------------------

Molecular and crystal geometries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. module:: caf.sci.geomlib

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

.. module:: caf.sci.aims

.. autoclass:: Aims
    :members:

.. autoclass:: SpeciesDefaults
    :members:

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
