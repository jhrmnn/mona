# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
class Linker:
    """Represents a dependency between tasks."""

    def __init__(self, name, *childlinks, parent=None, child=None, **kwargs):
        self.name = name
        self.childlinks = childlinks
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
                self.name,
                *self.childlinks,
                **self.kwargs
            )
            return self.parent
        return self


class TargetGen:
    """Represents a target."""

    def __init__(self, node, path):
        self.node = node
        self.path = path

    def __repr__(self):
        return f'TargetGen({self.node!r}, {repr(self.args)[1:-1]}, **{self.kwargs})'

    def __rmul__(self, task):
        if not isinstance(task, TaskGen):
            return NotImplemented
        self.node.set_task(task.node, self.path)
        return task

    __rmatmul__ = __rmul__


class TaskGen:
    """Represents a task."""

    def __init__(self, node):
        self.node = node

    def __repr__(self):
        return f'TaskGen({self.node!r})'

    def __radd__(self, obj):
        try:
            for x in obj:
                x + self
            return self
        except TypeError as e:
            return NotImplemented
