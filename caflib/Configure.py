from pathlib import Path
from io import StringIO

from caflib.Template import Template
from caflib.Utils import slugify, listify, timing
from caflib.Logging import error
from caflib.Generators import Linker, TargetGen, TaskGen


_features = {}


class UnconsumedAttributes(Exception):
    pass


class Feature:
    def __init__(self, name, f):
        self.name = name
        self.f = f
        self.attribs = set()

    def __call__(self, task):
        self.f(task)


def feature(name):
    def decorator(f):
        feat = Feature(name, f)
        _features[name] = feat
        return feat
    return decorator


def before_files(feat):
    feat.attribs.add('before_files')
    return feat


def before_templates(feat):
    feat.attribs.add('before_templates')
    return feat


class TargetNode:
    def __init__(self, name):
        self.name = name
        self.children = {}

    def __repr__(self):
        return f"<TargetNode '{self}'>"

    def __str__(self):
        return self.name

    def add_task(self, task, name=None):
        if name is not None:
            name = slugify(name)
        if name in self.children:
            error(f'"{name}" already in target "{self.name}"')
        self.children[name] = task
        task.parents.append(self)


class TaskNode:
    def __init__(self, task):
        self.task = task
        self.children = {}
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
                return f'{name}<-{parent!s}'

    def add_child(self, task, *args, blocks=False):
        if self is task:
            error(f'Task cannot depend on itself: {self}')
        if len(args) > 0:
            name, *symlinks = args
        else:
            name, symlinks = None, []
        if name is not None:
            name = slugify(name)
        else:
            for i in range(1, 10):
                name = f'_{i}'
                if name not in self.children:
                    break
            else:
                error(f'Task should not have more than 9 unnamed children: {self}')
        self.children[name] = task
        if blocks:
            self.blocking.append(name)
        if symlinks:
            for spec in symlinks:
                if isinstance(spec, tuple):
                    source, target = spec
                else:
                    source = target = spec
                self.task.symlink(Path(name)/source, target)
        task.parents.append(self)


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

    def consume(self, attr):
        """Return and clear a Task attribute."""
        return self.attrs.pop(attr, None)

    def open(self, filename, mode='r'):
        if mode in ['r', 'a']:
            if filename not in self.inputs:
                raise FileNotFoundError(filename)
        elif mode == 'w':
            pass
        else:
            error(f'Cannot open virtual files with mode {mode}')
        return VirtualTextFile(filename, self.inputs)

    def symlink(self, source, target):
        self.symlinks[str(target)] = str(source)

    def process(self, ctx):
        try:
            features = [
                _features[feat] if isinstance(feat, str)
                else Feature(feat.__name__, feat)
                for feat in listify(self.consume('features'))
            ]
        except KeyError as e:
            error(f'Feature {e.args[0]} is not registered')
        self.process_features(features, 'before_files')
        with timing('texts'):
            for text, target in (self.consume('texts') or {}).items():
                self.inputs[target] = text
        with timing('files'):
            for file_spec in listify(self.consume('files')):
                if isinstance(file_spec, tuple):
                    path, target = file_spec
                    self.inputs[target] = ctx.get_sources(path)[path]
                elif isinstance(file_spec, str):
                    path = file_spec
                    for path, contents in ctx.get_sources(path).items():
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
                template = Template(source)
                processed, used = template.substitute(self.attrs)
                self.inputs[target] = processed
                for attr in used:
                    self.consume(attr)
        self.process_features(features)
        self.command = self.consume('command')
        if self.attrs:
            raise UnconsumedAttributes(list(self.attrs))

    def process_features(self, features, attrib=None):
        with timing('features'):
            for feat in list(features):
                if not attrib or attrib in feat.attribs:
                    with timing(feat.name):
                        feat(self)
                    features.remove(feat)


class Context:
    """Represent a build configuration: tasks and targets."""

    def __init__(self, top):
        self.top = Path(top)
        self.tasks = []
        self.targets = {}
        self.files = {}

    def add_task(self, **attrs):
        task = Task(attrs)
        tasknode = TaskNode(task)
        self.tasks.append(tasknode)
        return TaskGen(tasknode)

    __call__ = add_task

    def link(self, *args, **kwargs):
        return Linker(*args, **kwargs)

    def target(self, name, *args, **kwargs):
        target = self.targets.get(name)
        if not target:
            target = TargetNode(name)
            self.targets[name] = target
        return TargetGen(target, *args, **kwargs)

    def get_sources(self, path):
        if '?' in path or '*' in path:
            paths = (self.top/path).glob()
        else:
            paths = self.top/path
        if not paths:
            error('File "{path}" does not exist.')
        for path in paths:
            if path not in self.files:
                with path.open() as f:
                    self.files[path] = f.read()
        return {
            str(path.relative_to(self.top)): self.files[path]
            for path in paths
        }

    def process(self):
        with timing('task processing'):
            for node in self.tasks:
                if not node.blocking:
                    node.task.process(self)

    def get_configuration(self):
        return {
            'tasks': [(
                {
                    'command': node.task.command,
                    'inputs': node.task.inputs,
                    'children': {
                        name: self.tasks.index(child)
                        for name, child in node.children.items()
                    }
                } if not node.task.attrs else {
                    'attributes': {
                        name: repr(attrs)
                        for name, attrs in node.task.attrs.items()
                    }
                }
            ) for node in self.tasks],
            'targets': {
                target.name: {
                    taskname: self.tasks.index(task)
                    for taskname, task in target.children.items()
                } if list(target.children.keys()) != [None]
                else self.tasks.index(target.children[None])
                for target in self.targets.values()
            }
        }

    def load_tool(self, name):
        __import__('caflib.Tools.' + name)
