from progressbar import ProgressBar

import os
import hashlib
import shutil
import json
from glob import glob
from collections import defaultdict, namedtuple
from pathlib import Path
from math import log10, ceil

from caflib.Utils import mkdir, slugify, cd, listify
from caflib.Template import Template
from caflib.Logging import warn, info, error

_features = {}


def feature(name):
    """Register function as a feature in Context.

    Example:

        @feature('myfeat')
        def my_feature(...
    """
    def decorator(f):
        _features[name] = f
        return f
    return decorator


def str_to_path(s, nlvls=2, lenlvl=2):
    """Return relative path constructed from a sttring.

    Example:

        str_to_path('abcdefghij', 3, 1) -> a/b/c/defghij
    """
    levels = []
    for lvl in range(nlvls):
        levels.append(s[lvl*lenlvl:(lvl+1)*lenlvl])
    levels.append(s[nlvls*lenlvl:])
    path = Path(levels[0])
    for l in levels[1:]:
        path = path/l
    return path


def get_file_hash(path, hashf='sha1'):
    """Return hashed contents of a file."""
    h = hashlib.new(hashf)
    with path.open('rb') as f:
        h.update(f.read())
    return h.hexdigest()


class Task:
    """Represents a single build task."""

    def __init__(self, **attrs):
        self.attrs = attrs
        self.files = {}
        self.children = []
        self.parents = []
        self.targets = []
        self.links = {}
        self._parent_counter = 0

    def __radd__(self, iterable):
        try:
            for x in iterable:
                x + self
            return self
        except TypeError:
            return NotImplemented

    def __repr__(self):
        try:
            up = self.parents[-1] if self.parents else self.targets[-1]
        except IndexError:
            up = ('?', None)
        if up[1]:
            return '{0[1]}<-{0[0]!s}'.format(up)
        else:
            return '{0[0]!s}'.format(up)

    def consume(self, attr):
        """Return and clear a Task attribute."""
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
        self.children.append(task)
        linkname = slugify(link)
        self.links[slugify(link)] = Task.Link(task, links, needed)
        task.parents.append((self, linkname))
        return self

    def link_deps(self):
        with cd(self.path):
            for linkname, link in self.links.items():
                os.system('ln -fns {} {}'
                          .format(os.path.relpath(str(link.task.path)),
                                  linkname))
            for filename, path in self.files.items():
                os.system('ln -fns {} {}'
                          .format(os.path.relpath(str(path)),
                                  filename))

    def store_link_file(self, source, target=None):
        if not target:
            target = source
        filehash = get_file_hash(Path(source))
        cellarpath = self.ctx.cellar/str_to_path(filehash)
        if not cellarpath.is_file():
            info('Stored new file {}'.format(source))
            mkdir(cellarpath.parent, parents=True)
            shutil.copy(source, str(cellarpath))
        with cd(self.path):
            os.system('ln -fns {} {}'
                      .format(os.path.relpath(str(cellarpath)), target))
        self.files[target] = cellarpath

    def prepare(self):
        """Prepare a task.

        Pull in files and templates, link in files from children, execute
        features and save the command. Check that all attributes have been
        consumed.
        """
        for filename in listify(self.consume('files')):
            if isinstance(filename, tuple):
                self.store_link_file(filename[0], filename[1])
            else:
                if '*' in filename or '?' in filename:
                    for member in glob(filename):
                        self.store_link_file(member)
                else:
                    self.store_link_file(filename)
        templates = [Template(path) for path in listify(self.consume('templates'))]
        with cd(self.path):
            for template in templates:
                used = template.substitute(self.attrs)
                for attr in used:
                    self.consume(attr)
            for linkname, link in self.links.items():
                for symlink in link.links:
                    if isinstance(symlink, tuple):
                        target, symlink = symlink
                    else:
                        target = symlink
                    os.system('ln -s {}/{} {}'
                              .format(linkname, target, symlink))
            for feat in listify(self.consume('features')):
                if isinstance(feat, str):
                    feat = _features[feat]
                feat(self)
            command = self.consume('command')
            if command:
                with open('command', 'w') as f:
                    f.write(command)
            if self.attrs:
                raise RuntimeError('task has non-consumed attributs {}'
                                   .format(list(self.attrs)))

    def get_hashes(self):
        """Get hashes of task's dependencies.

        Dependencies consist of all files and on locks of children.
        """
        with cd(self.path):
            hashes = {}
            for dirpath, dirnames, filenames in os.walk('.'):
                if dirpath == '.':
                    dirnames[:] = [name for name in dirnames
                                   if name not in ['.caf'] + list(self.links)]
                for name in filenames:
                    filepath = Path(dirpath)/name
                    if filepath.is_symlink():
                        target = os.readlink(str(filepath))
                        if Path(target).is_absolute():
                            hashes[str(filepath)] = get_file_hash(Path(target))
                        elif str(filepath) in self.files:
                            hashes[str(filepath)] = get_file_hash(Path(target))
                        else:
                            hashes[str(filepath)] = target
                    else:
                        hashes[str(filepath)] = get_file_hash(filepath)
            for linkname in self.links:
                hashes[linkname] = get_file_hash(Path(linkname)/'.caf/lock')
        return hashes

    def set_path(self, path):
        self.path = Path(path).resolve()

    def build(self):
        """Prepare, lock and store the task.

        Check if not already locked. Touch (link in children, save chilren to
        .caf/children). Check if needed children are already sealed. Check if
        children are already locked. Prepare task. Get hashes.  Lock task with
        hashes. Check if a task has been already stored previously and if not,
        store it and relink children.
        """
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
        self.prepare()
        hashes = self.get_hashes()
        self.lock(hashes)
        if 'command' not in hashes:
            with (self.path/'.caf/seal').open('w') as f:
                print('build', file=f)
        myhash = get_file_hash(self.path/'.caf/lock')
        cellarpath = self.ctx.cellar/str_to_path(myhash)
        if cellarpath.is_dir():
            shutil.rmtree(str(self.path))
        else:
            info('Stored new task {}'.format(self))
            mkdir(cellarpath.parent, parents=True)
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

    def __repr__(self):
        return '{}.{}({}, *{}, **{})'.format(getattr(self, 'x', None),
                                             self.fname,
                                             getattr(self, 'y', None),
                                             self.args,
                                             self.kwargs)

    def __add__(self, x):
        if hasattr(self, 'x'):
            return NotImplemented
        self.x = x
        self.last = 'x'
        return self.run()

    def __radd__(self, y):
        if hasattr(self, 'y'):
            return NotImplemented
        self.y = y
        self.last = 'y'
        return self.run()

    def run(self):
        try:
            val = getattr(self.x, self.fname)(self.y, *self.args, **self.kwargs)
            delattr(self, self.last)
            return val
        except AttributeError:
            return self


class Link(AddWrapper):
    """Represents dependency links between tasks."""

    def __init__(self, *args, **kwargs):
        return super().__init__('add_dependency', *args, **kwargs)

    def __add__(self, x):
        if not isinstance(x, Task):
            return NotImplemented
        return super().__add__(x)

    def __radd__(self, y):
        if not isinstance(y, Task):
            return NotImplemented
        return super().__radd__(y)


class Target(AddWrapper):
    """Represents adding a task to a build context as a target."""

    def __init__(self, *args, **kwargs):
        return super().__init__('add_to_target', *args, **kwargs)

    def __add__(self, x):
        if not isinstance(x, Context):
            return NotImplemented
        return super().__add__(x)

    def __radd__(self, y):
        if not isinstance(y, Task):
            return NotImplemented
        return super().__radd__(y)


class Context:
    """Represent a complete build: tasks and targets."""

    def __init__(self, cellar):
        try:
            self.cellar = cellar.resolve()
        except FileNotFoundError:
            error('Cellar does not exist, maybe `caf init` first?')
        self.tasks = []
        self.targets = defaultdict(dict)

    def add_task(self, **kwargs):
        task = Task(**kwargs)
        task.ctx = self
        self.tasks.append(task)
        return task

    __call__ = add_task

    def add_to_target(self, task, target, link=None):
        linkname = slugify(link) if link else None
        if linkname in self.targets[target]:
            error('Link {} already in target {}'.format(linkname, target))
        self.targets[target][linkname] = task
        task.targets.append((target, linkname))
        return task

    def link(self, *args, **kwargs):
        link = Link(*args, **kwargs)
        return link

    def target(self, *args, **kwargs):
        return Target(*args, **kwargs) + self

    def sort_tasks(self):
        """Sort tasks such that children precede parents (topological sort)."""
        queue = []
        tops = [task for task in self.tasks if not task.parents]
        while tops:
            node = tops.pop()
            queue.insert(0, node)
            for child in node.children:
                child._parent_counter += 1
                if child._parent_counter == len(child.parents):
                    tops.append(child)
        assert all(task._parent_counter == len(task.parents)
                   for task in self.tasks)
        self.tasks = queue

    def build(self, batch):
        try:
            batch = batch.resolve()
        except FileNotFoundError:
            error('Batch does not exist, maybe `caf build new` first?')
        self.sort_tasks()
        ntskdigit = ceil(log10(len(self.tasks)+1))
        with ProgressBar(maxval=len(self.tasks)) as progress:
            for i, task in enumerate(self.tasks):
                path = batch/'{:0{n}d}'.format(i, n=ntskdigit)
                if not path.is_dir():
                    mkdir(path)
                task.set_path(path)
                task.build()
                progress.update(i)

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
