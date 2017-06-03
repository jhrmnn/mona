# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
from io import StringIO
import json
import os
import importlib

from caflib.Template import Template
from caflib.Utils import listify, slugify
from caflib.Timing import timing
from caflib.Logging import error
from caflib.Cellar import get_hash, State


class UnconsumedAttributes(Exception):
    pass


class FeatureException(Exception):
    pass


class Feature:
    db = {}

    def __init__(self, name, f, attrs=None):
        self.name = name
        self.f = f
        self.attrs = attrs or set()

    def __call__(self, task):
        self.f(task)

    @classmethod
    def ensure_feature(cls, obj):
        if type(obj) is cls:
            return obj
        if type(obj) is str:
            return cls.db[obj]
        return cls(obj.__name__, obj)


def feature(name):
    def decorator(f):
        feat = Feature(name, f)
        Feature.db[name] = feat
        return feat
    return decorator


def before_files(feat):
    feat = Feature.ensure_feature(feat)
    feat.attrs.add('before_files')
    return feat


def before_templates(feat):
    feat = Feature.ensure_feature(feat)
    feat.attrs.add('before_templates')
    return feat


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


class TaskNode:
    hashes = {}

    def __init__(self, task):
        self.task = task
        self.children = {}
        self.childlinks = {}
        self.parents = []
        self.blocking = []

    def __repr__(self):
        return f"<TaskNode '{self}'>"

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
        for filename, content in self.task.inputs.items():
            hashid = get_hash(content)
            if hashid not in inputs:
                inputs[hashid] = content
            self.task.inputs[filename] = hashid
        blob = json.dumps({
            'command': self.task.command,
            'inputs': self.task.inputs,
            'symlinks': self.task.symlinks,
            'children': {
                name: TaskNode.hashes[child]
                for name, child in self.children.items()
            },
            'childlinks': self.childlinks
        }, sort_keys=True)
        myhash = get_hash(blob)
        TaskNode.hashes[self] = myhash


class VirtualTextFile(StringIO):
    def __init__(self, name, inputs):
        super().__init__(inputs.get(name))
        self.name = name
        self.inputs = inputs

    def __exit__(self, *args, **kwargs):
        self.inputs[self.name] = self.getvalue()
        super().__exit__(*args, **kwargs)


class Task:
    def __init__(self, attrs):
        self.attrs = attrs
        self.command = None
        self.inputs = {}
        self.symlinks = {}

    def consume(self, attr, default=None):
        """Return and clear a Task attribute."""
        return self.attrs.pop(attr, default)

    def open(self, filename, mode='r'):
        if mode == 'r':
            with timing('node_open'):
                fobj = self.node_open(filename)
            if fobj:
                return fobj
        if mode in ['r', 'a']:
            if filename not in self.inputs:
                raise FileNotFoundError(filename)
        elif mode == 'w':
            pass
        else:
            error(f'Cannot open files with mode {mode}')
        return VirtualTextFile(filename, self.inputs)

    def symlink(self, source, target):
        self.symlinks[str(target)] = str(source)

    def process(self, ctx):
        try:
            features = [
                Feature.ensure_feature(feat)
                for feat in listify(self.consume('features'))
            ]
        except KeyError as e:
            error(f'Feature {e.args[0]} is not registered')
        self.process_features(features, 'before_files')
        with timing('texts'):
            for target, text in (self.consume('texts') or {}).items():
                self.inputs[target] = text
        with timing('files'):
            for file_spec in listify(self.consume('files')):
                if isinstance(file_spec, tuple):
                    path, target = file_spec
                    self.inputs[target] = ctx.get_sources(ctx.top/path)[path]
                elif isinstance(file_spec, str):
                    path = file_spec
                    for path, contents in ctx.get_sources(ctx.top/path).items():
                        self.inputs[path] = contents
                else:
                    error('Unexpected file specification: {file_spec}')
        self.process_features(features, 'before_templates')
        with timing('templates'):
            for file_spec in listify(self.consume('templates')):
                if isinstance(file_spec, tuple):
                    source, target = file_spec
                elif isinstance(file_spec, str):
                    source = target = file_spec
                else:
                    error('Unexpected template specification: {file_spec}')
                if isinstance(source, os.PathLike):
                    source = ctx.top/source
                template = Template(source)
                processed, used = template.render(self.attrs)
                self.inputs[target] = processed
                for attr in used:
                    self.consume(attr)
        self.process_features(features)
        self.command = self.consume('command') or ''
        if self.attrs:
            raise UnconsumedAttributes(list(self.attrs))

    def process_features(self, features, attr=None):
        with timing('features'):
            for feat in list(features):
                if not attr or attr in feat.attrs:
                    with timing(feat.name):
                        try:
                            feat(self)
                        except Exception as e:
                            import traceback
                            traceback.print_exc()
                            raise FeatureException(feat.name) from e
                    features.remove(feat)


class VirtualFile:
    def __init__(self, hashid, cellar):
        self.hashid = hashid
        self.cellar = cellar

    @property
    def path(self):
        return self.cellar.get_file(self.hashid)


class TaskWrapper:
    def __init__(self, node, cellar):
        self.node = node
        self.cellar = cellar

    @property
    def hashid(self):
        return TaskNode.hashes[self.node]

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
        kwargs.setdefault('features', [])
        task = Task(kwargs)
        tasknode = TaskNode(task)
        self.tasks.append(tasknode)
        if children:
            for childname, (child, childlinks) in children.items():
                tasknode.add_child(child.node, childname, *childlinks)
        if target:
            targetnode = TargetNode()
            self.targets.append(targetnode)
            targetnode.set_task(tasknode, target)
        tasknode.task.process(self)
        tasknode.seal(self.inputs)
        return TaskWrapper(tasknode, self.cellar)

    def get_sources(self, path):
        if '?' in str(path) or '*' in str(path):
            paths = path.glob()
        else:
            paths = [path]
        if not paths:
            error(f'File "{path}" does not exist.')
        for path in paths:
            if path not in self.files:
                self.files[path] = path.read_text()
        return {
            str(path.relative_to(self.top)): self.files[path]
            for path in paths
        }

    def get_configuration(self):
        idxs = {task: i for i, task in enumerate(self.tasks)}
        return {
            'tasks': [(
                {
                    'command': node.task.command,
                    'inputs': node.task.inputs,
                    'symlinks': node.task.symlinks,
                    'children': {
                        name: idxs[child]
                        for name, child in node.children.items()
                    },
                    'childlinks': node.childlinks
                } if node.task.command is not None else {
                    'blocking': node.blocking,
                    'children': {
                        name: idxs[child]
                        for name, child in node.children.items()
                    }
                }
            ) for node in self.tasks],
            'hashes': [TaskNode.hashes.get(node) for node in self.tasks],
            'targets': {
                str(target.path): self.tasks.index(target.task)
                for target in self.targets
            },
            'labels': [str(node) for node in self.tasks]
        }

    def load_tool(self, name):
        importlib.import_module(f'caflib.Tools.{name}')
