Glossary
========

.. glossary::

    task
        The basic building block of a calculation. A task consists of a Python
        function and the arguments passed into it (inputs). The result of
        a task is the return value of the function (output). A task depends on
        other tasks when its inputs reference outputs of other tasks. When
        run, a task can create new tasks, and its output may reference outputs
        of those tasks.

    rule
        Recipe for creating tasks. Calling a rule with soem arguments creates
        a task with those arguments as inputs.

    task factory
        Generalization of a rule that returns a task when called, but may first
        preprocess the arguments before assigning them as inputs to the task.

    directory task
        A special kind of file-based :term:`task` which has an executable instead of
        a Python function and files instead of arguments, and the output is
        a collection of files generated by the executable when run in
        a temporary directory with the input files present.
