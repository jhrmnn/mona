import os
import hashlib
import shutil
import json
from collections import defaultdict, namedtuple
from pathlib import Path
from math import log10, ceil

from caflib.Utils import mkdir, slugify, cd, listify
from caflib.Template import Template
from caflib.Logging import warn, info

_features = {}


def feature(name):
    def decorator(f):
        _features[name] = f
        return f
    return decorator


def hash_to_path(sha, nlvls=2, lenlvl=2):
    levels = []
    for lvl in range(nlvls):
        levels.append(sha[lvl*lenlvl:(lvl+1)*lenlvl])
    levels.append(sha[nlvls*lenlvl:])
    path = Path(levels[0])
    for l in levels[1:]:
        path = path/l
    return path


def get_file_hash(path):
    h = hashlib.new('sha1')
    with path.open('rb') as f:
        h.update(f.read())
    return h.hexdigest()


class Task:
    def __init__(self, **attrs):
        self.attrs = attrs
        self.children = []
        self.parents = []
        self.links = {}

    def consume(self, attr):
        return self.attrs.pop(attr, None)

    def is_touched(self):
        return (self.path/'.caf/children').is_file()

    def is_locked(self):
        return (self.path/'.caf/lock').is_file()

    def is_sealed(self):
        return (self.path/'.caf/seal').is_file()

    def touch(self):
        mkdir(self.path/'.caf')
        with (self.path/'.caf/children').open('w') as f:
            json.dump(list(self.links), f, sort_keys=True)
        self.link_deps()

    def lock(self, hashes):
        with (self.path/'.caf/lock').open('w') as f:
            json.dump(hashes, f, sort_keys=True)

    Link = namedtuple('Link', 'task links needed')

    def add_dependency(self, task, link, *links, needed=False):
        if not isinstance(task, Task):
            return NotImplemented
        self.children.append(task)
        self.links[slugify(link)] = Task.Link(task, links, needed)
        task.parents.append(self)
        return self

    def __radd__(self, iterable):
        try:
            for x in iterable:
                x + self
            return self
        except TypeError:
            return NotImplemented

    def link_deps(self):
        with cd(self.path):
            for linkname, link in self.links.items():
                os.system('ln -fns {} {}'
                          .format(os.path.relpath(str(link.task.path)),
                                  linkname))

    def prepare(self):
        for filename in listify(self.consume('files')):
            if isinstance(filename, tuple):
                shutil.copy(filename[0], str(self.path/filename[1]))
            else:
                shutil.copy(filename, str(self.path))
        templates = [Template(path) for path in listify(self.consume('templates'))]
        with cd(self.path):
            for template in templates:
                used = template.substitute(self.attrs)
                for used_key in used:
                    self.consume(used_key)
            for linkname, link in self.links.items():
                for symlink in link.links:
                    try:
                        symlink, target = symlink
                    except ValueError:
                        target = symlink
                    os.system('ln -s {}/{} {}'
                              .format(linkname, target, symlink))
            for feat in listify(self.consume('features')):
                if isinstance(feat, str):
                    feat = _features[feat]
                feat(self)
            with open('command', 'w') as f:
                f.write(self.consume('command'))
            if self.attrs:
                raise RuntimeError('task has non-consumed attributs {}'
                                   .format(list(self.attrs)))

    def get_hashes(self):
        with cd(self.path):
            filepaths = []
            for dirpath, dirnames, filenames in os.walk('.'):
                if dirpath == '.':
                    dirnames[:] = [name for name in dirnames
                                   if name not in ['.caf'] + list(self.links)]
                for name in filenames:
                    filepath = Path(dirpath)/name
                    if not filepath.is_symlink():
                        filepaths.append(filepath)
            for linkname in self.links:
                filepaths.append(Path(linkname)/'.caf/lock')
            hashes = {}
            for path in filepaths:
                hashes[str(path)] = get_file_hash(path)
        return hashes

    def set_path(self, path):
        self.path = Path(path).resolve()

    def build(self):
        if self.is_locked():
            warn('{} already locked'.format(self))
            return
        if not self.is_touched():
            self.touch()
        for linkname, link in self.links.items():
            if link.needed and not link.task.is_sealed():
                warn('{}: dependency {!r} not sealed'.format(self, linkname))
                return
        if not all(child.is_locked() for child in self.children):
            return
        info('Preparing task {}...'.format(self))
        self.prepare()
        hashes = self.get_hashes()
        self.lock(hashes)
        myhash = get_file_hash(self.path/'.caf/lock')
        cellarpath = self.ctx.cellar/hash_to_path(myhash)
        if cellarpath.is_dir():
            info('{} already stored'.format(self))
            shutil.rmtree(str(self.path))
        else:
            mkdir(cellarpath.parent, p=True)
            self.path.rename(cellarpath)
        self.path.symlink_to(cellarpath)
        self.path = cellarpath
        self.link_deps()


class AddWrapper:

    """Wraps `x.f(y, *args, **kwargs)` into `y + Wrapper('f', *args, **kwargs) + x`."""

    def __init__(self, fname, *args, **kwargs):
        self.fname = fname
        self.args = args
        self.kwargs = kwargs

    def __add__(self, x):
        if hasattr(self, 'x'):
            return NotImplemented
        self.x = x
        return self.run()

    def __radd__(self, y):
        if hasattr(self, 'y'):
            return NotImplemented
        self.y = y
        return self.run()

    def run(self):
        try:
            return getattr(self.x, self.fname)(self.y, *self.args, **self.kwargs)
        except AttributeError:
            return self


class Link(AddWrapper):
    def __init__(self, *args, **kwargs):
        return super().__init__('add_dependency', *args, **kwargs)

    def __add__(self, x):
        if isinstance(x, Link):
            return NotImplemented
        return super().__add__(x)

    def __radd__(self, y):
        if isinstance(y, Link):
            return NotImplemented
        return super().__radd__(y)


class Target(AddWrapper):
    def __init__(self, *args, **kwargs):
        return super().__init__('add_to_target', *args, **kwargs)


class Context:
    def __init__(self, cellar):
        self.tasks = []
        self.targets = defaultdict(dict)
        self.cellar = cellar

    def add_task(self, **kwargs):
        task = Task(**kwargs)
        task.ctx = self
        self.tasks.append(task)
        return task

    __call__ = add_task

    def add_to_target(self, task, target, link=None):
        if not isinstance(task, Task):
            return NotImplemented
        self.targets[target][slugify(link)] = task
        return task

    def link(self, *args, **kwargs):
        link = Link(*args, **kwargs)
        return link

    def target(self, *args, **kwargs):
        return Target(*args, **kwargs) + self

    def sort_tasks(self):
        queue = []

        def enqueue(task):
            if task not in queue:
                queue.append(task)
            for child in task.children:
                enqueue(child)

        for task in self.tasks:
            if not task.parents:
                enqueue(task)
        self.tasks = reversed(queue)

    def build(self, brewery):
        self.cellar = self.cellar.resolve()
        brewery = brewery.resolve()
        ntskdigit = ceil(log10(len(self.tasks)+1))
        for i, task in enumerate(self.tasks):
            path = brewery/'{:0{n}d}'.format(i, n=ntskdigit)
            if not path.is_dir():
                mkdir(path)
            task.set_path(path)
            task.build()

    def make_targets(self, out):
        for target, tasks in self.targets.items():
            if len(tasks) == 1 and None in tasks:
                os.system('ln -fns {} {}'.format(tasks[None].path, out/target))
            else:
                if not (out/target).is_dir():
                    mkdir(out/target)
                for name, task in tasks.items():
                    os.system('ln -fns {} {}'.format(task.path, out/target/name))

    def load_tool(self, name):
        __import__('caflib.Tools.' + name)
