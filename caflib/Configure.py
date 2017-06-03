# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
from io import StringIO
import json

from caflib.Utils import slugify
from caflib.Logging import error
from caflib.Cellar import get_hash, State


class TargetNode:
    all_targets = set()

    def __init__(self):
        self.path = None

    @property
    def children(self):
        return {self.path.name: self.task}

    def __repr__(self):
        return f"<TargetNode '{self.path}'>"

    def __str__(self):
        return f'{self.path.parent}' if len(self.path.parts) > 1 else ''

    def set_task(self, task, path):
        try:
            self.path = Path(slugify(path, path=True))
        except TypeError:
            error(f'Target path {path!r} is not a string')
        if self.path in TargetNode.all_targets:
            error(f'Multiple definitions of target "{self.path}"')
        TargetNode.all_targets.add(self.path)
        self.task = task
        task.parents.append(self)


class Task:
    hashes = {}

    def __init__(self, cellar, command):
        self.cellar = cellar
        self.command = command
        self.inputs = {}
        self.children = {}
        self.childlinks = {}
        self.parents = []
        self.blocking = []

    def __repr__(self):
        return f"<Task '{self}'>"

    def __str__(self):
        if not self.parents:
            return '?'
        parent = self.parents[-1]
        for name, child in parent.children.items():
            if child is self:
                par = str(parent)
                return f'{par}/{name}' if par else name

    def add_child(self, task, name, *childlinks, blocks=False):
        if self is task:
            error(f'Task cannot depend on itself: {self}')
        try:
            name = slugify(name)
        except TypeError:
            error(f'Dependency name {name!r} is not a string')
        if name in self.children:
            error(f'Task already has child {name}: {self}')
        self.children[name] = task
        if blocks:
            self.blocking.append(name)
        if childlinks:
            for spec in childlinks:
                if isinstance(spec, tuple):
                    source, target = spec
                else:
                    source = target = spec
                self.childlinks[target] = (name, source)
        task.parents.append(self)

    def seal(self, inputs):
        for filename, content in self.inputs.items():
            hashid = get_hash(content)
            if hashid not in inputs:
                inputs[hashid] = content
            self.inputs[filename] = hashid
        blob = json.dumps({
            'command': self.command,
            'inputs': self.inputs,
            'symlinks': {},
            'children': {
                name: Task.hashes[child]
                for name, child in self.children.items()
            },
            'childlinks': self.childlinks
        }, sort_keys=True)
        myhash = get_hash(blob)
        Task.hashes[self] = myhash

    @property
    def hashid(self):
        return Task.hashes[self]

    @property
    def state(self):
        return self.cellar.get_state(self.hashid)

    @property
    def finished(self):
        return self.state == State.DONE

    @property
    def outputs(self):
        return {
            name: VirtualFile(hashid, self.cellar) for name, hashid
            in self.cellar.get_task(self.hashid)['outputs'].items()
        }


class VirtualTextFile(StringIO):
    def __init__(self, name, inputs):
        super().__init__(inputs.get(name))
        self.name = name
        self.inputs = inputs

    def __exit__(self, *args, **kwargs):
        self.inputs[self.name] = self.getvalue()
        super().__exit__(*args, **kwargs)


class VirtualFile:
    def __init__(self, hashid, cellar):
        self.hashid = hashid
        self.cellar = cellar

    @property
    def path(self):
        return self.cellar.get_file(self.hashid)


def get_configuration(tasks, targets):
    idxs = {task: i for i, task in enumerate(tasks)}
    return {
        'tasks': [(
            {
                'command': node.command,
                'inputs': node.inputs,
                'symlinks': {},
                'children': {
                    name: idxs[child]
                    for name, child in node.children.items()
                },
                'childlinks': node.childlinks
            } if node.command is not None else {
                'blocking': node.blocking,
                'children': {
                    name: idxs[child]
                    for name, child in node.children.items()
                }
            }
        ) for node in tasks],
        'hashes': [Task.hashes.get(node) for node in tasks],
        'targets': {
            str(target.path): tasks.index(target.task)
            for target in targets
        },
        'labels': [str(node) for node in tasks]
    }


class Context:
    """Represent a build configuration: tasks and targets."""

    def __init__(self, top, cellar):
        self.top = Path(top)
        self.cellar = cellar
        self.tasks = []
        self.targets = []
        self.files = {}
        self.inputs = {}

    def get_task(self, target=None, children=None, **kwargs):
        tasknode = Task(self.cellar, **kwargs)
        self.tasks.append(tasknode)
        if children:
            for childname, (child, childlinks) in children.items():
                tasknode.add_child(child, childname, *childlinks)
        if target:
            targetnode = TargetNode()
            self.targets.append(targetnode)
            targetnode.set_task(tasknode, target)
        tasknode.seal(self.inputs)
        return tasknode
