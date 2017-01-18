class AbstractTask:
    pass


class TaskGen(AbstractTask):
    """Placehoder representing a task in a cscript."""

    def __init__(self, task_node):
        self._task_node = task_node

    def __radd__(self, obj):
        if isinstance(obj, TaskGen):
            return obj + Linker() + self
        try:
            for x in obj:
                x + self
            return self
        except TypeError:
            return NotImplemented


class TaskNode(AbstractTask):
    def __init__(self, task_shell):
        self._task_shell = task_shell
        self.children = {}
        self.parents = []
        self.blocking = []

    def __repr__(self):
        if not self.parents:
            return '?'
        parent = self.parents[-1]
        for name, child in parent.children.items():
            if child is self:
                return f'{name}<-{parent!r}'

    def add_child(self, task, name=None, blocks=False):
        if self is task:
            error(f'Task cannot depend on itself: {self}')
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
        task.parents.append(self)


class VirtualTextFile(StringIO):
    def __init__(self, name, inputs):
        super().__init__(inputs.get(name))
        self.name
        self.inputs = inputs

    def __exit__(self, *args, **kwargs):
        self.inputs[self.name] = self.getvalue()
        super().__exit__(*args, **kwargs)


class TaskShell(AbstractTask):
    def __init__(self, attrs):
        self.attrs = attrs
        self.inputs = {}

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


def process_task(task, ctx):
    try:
        features = [
            ctx.features[feat] if isinstance(feat, str)
            else Feature(feat.__name__, feat)
            for feat in listify(task.consume('features'))
        ]
    except KeyError as e:
        error(f'Feature {e.args[0]} is not registered')
    process_features(task, features, 'before_files')
    with timing('files'):
        for file_spec in listify(task.consume('files')):
            if isinstance(file_spec, tuple):
                source, target = file_spec
                task.inputs[target] = ctx.get_sources(source)[source]
            elif isinstance(file_spec, str):
                sources = file_spec
                for filename, contents in ctx.get_sources(sources).items():
                    task.inputs[filename] = contents
            else:
                error('Unexpected file specification: {file_spec}')
    process_features(task, features, 'before_templates')
    with timing('templates'):
        templates = {}
        for file_spec in listify(task.consume('templates')):
            if isinstance(file_spec, tuple):
                source, target = file_spec
            elif isinstance(file_spec, str):
                source = target = filename
            else:
                error('Unexpected template specification: {file_spec}')
            template = ctx.get_template(source)
            processed, used = template.substitute(task.attrs)
            task.inputs[target] = processed
            for attr in used:
                task.consume(attr)
    process_features(task, features)
    command = task.consume('command')
    if task.attrs:
        raise UnconsumedAttributes(list(task.attrs))
    return {'command': command, 'inputs': self.inputs}


def process_features(task, features, attrib=None):
    with timing('features'):
        for feat in list(features):
            if not attrib or attrib in feat.attribs:
                with timing(name):
                    feat(task)
                features.remove(feat)
