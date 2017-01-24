class Linker:
    """Represents a dependency between tasks."""

    def __init__(self, *args, parent=None, child=None, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.parent = parent
        self.child = child

    def __repr__(self):
        return f'Linker({repr(self.args)[1:-1]}, parent={self.parent!r}, ' \
            f'child={self.child!r}, **{self.kwargs!r})'

    def __add__(self, parent):
        if self.parent or not isinstance(parent, TaskGen):
            return NotImplemented
        self.parent = parent
        return self._run()

    def __radd__(self, child):
        if self.child or not isinstance(child, TaskGen):
            return NotImplemented
        self.child = child
        return self._run()

    def _run(self):
        if self.child and self.parent:
            self.parent.node.add_child(
                self.child.node,
                *self.args,
                **self.kwargs
            )
            return self.parent
        return self


class TargetGen:
    """Represents a target."""

    def __init__(self, node, *args, **kwargs):
        self.node = node
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return f'TargetGen({self.node!r}, {repr(self.args)[1:-1]}, **{self.kwargs})'

    def __rmul__(self, task):
        if not isinstance(task, TaskGen):
            return NotImplemented
        self.node.set_task(task.node, *self.args, **self.kwargs)
        return task

    __rmatmul__ = __rmul__


class TaskGen:
    """Represents a task."""

    def __init__(self, node):
        self.node = node

    def __repr__(self):
        return f'TaskGen({self.node!r})'

    def __radd__(self, obj):
        if isinstance(obj, TaskGen):
            return obj + Linker() + self
        try:
            for x in obj:
                x + self
            return self
        except TypeError:
            return NotImplemented
