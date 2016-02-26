from progressbar import ProgressBar

import os
import hashlib
import shutil
import json
from glob import glob
from collections import defaultdict, namedtuple
from pathlib import Path
from math import log10, ceil

from caflib.Utils import mkdir, slugify, cd, listify, timing, relink, \
    make_nonwritable
from caflib.Template import Template
from caflib.Hook import process_hook
from caflib.Logging import warn, info, error

hashf = 'sha1'

_features = {}
_reports = []


def get_stored(path, sha=False, rel=False, require=True):
    full_path = Path(path).resolve()
    if len(full_path.parts) > 3 and full_path.parts[-4] == 'Cellar':
        if sha:
            return ''.join(full_path.parts[-3:])
        elif rel:
            return '/'.join(full_path.parts[-3:])
        else:
            return full_path
    else:
        if require:
            error('Path {} must be stored in cellar'.format(path))
        else:
            return None


def feature(name):
    """Register function as a feature in Context.

    Example:

        @feature('myfeat')
        def my_feature(...
    """
    def decorator(f):
        _features[name] = f
        f.feature_attribs = set()
        return f
    return decorator


def before_files(f):
    if not hasattr(f, 'feature_attribs'):
        f.feature_attribs = set()
    f.feature_attribs.add('before_files')
    return f


def before_templates(f):
    if not hasattr(f, 'feature_attribs'):
        f.feature_attribs = set()
    f.feature_attribs.add('before_templates')
    return f


def report(f):
    """Register function as a report in Context.

    Example:

        @report
        def my_report(...
    """
    _reports.append(f)
    return f


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


def get_file_hash(path):
    """Return hashed contents of a file."""
    h = hashlib.new(hashf)
    try:
        with path.open('rb') as f:
            h.update(f.read())
    except FileNotFoundError:
        error('File "{}" does not exist'.format(path))
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
        self.noname_link_counter = 0

    def __add__(self, iterable):
        try:
            for x in iterable:
                self + x
            return iterable
        except TypeError:
            return NotImplemented

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
        if self == task:
            error('Task cannot depend on itself: {}'.format(self))
        self.children.append(task)
        if link:
            linkname = slugify(link)
        else:
            self.noname_link_counter += 1
            linkname = '.{}'.format(self.noname_link_counter)
        self.links[linkname] = Task.Link(task, links, needed)
        task.parents.append((self, linkname))
        return self

    def link_deps(self):
        with cd(self.path):
            for linkname, link in self.links.items():
                relink(os.path.relpath(str(link.task.path)), linkname)
            for filename, path in self.files.items():
                try:
                    relink(os.path.relpath(str(path)), filename)
                except FileExistsError:
                    if 'RELINK' in os.environ:
                        Path(filename).unlink()
                        relink(os.path.relpath(str(path)), filename)
                    else:
                        error('Something replaced a linked file "{}" with a real file in {}'
                              .format(filename, self))

    def store_link_file(self, source, target=None):
        if not target:
            target = source
        filehash = get_file_hash(Path(source))
        cellarpath = self.ctx.cellar/str_to_path(filehash)
        if not cellarpath.is_file():
            info('Stored new file "{}"'.format(source))
            mkdir(cellarpath.parent, parents=True, exist_ok=True)
            shutil.copy(str(source), str(cellarpath))
            make_nonwritable(cellarpath)
        with cd(self.path):
            relink(os.path.relpath(str(cellarpath)), target)
        self.files[target] = cellarpath

    def store_link_text(self, text, target, label=None):
        h = hashlib.new(hashf)
        h.update(text.encode())
        texthash = h.hexdigest()
        cellarpath = self.ctx.cellar/str_to_path(texthash)
        if not cellarpath.is_file():
            if label is True:
                info('Stored new file "{}"'.format(target))
            elif label:
                info('Stored new text labeled "{}"'.format(label))
            else:
                info('Stored new text')
            mkdir(cellarpath.parent, parents=True, exist_ok=True)
            with cellarpath.open('w') as f:
                f.write(text)
            make_nonwritable(cellarpath)
        with cd(self.path):
            relink(os.path.relpath(str(cellarpath)), target)
        self.files[target] = cellarpath

    def process_features(self, features, attrib=None):
        with timing('features'):
            for name, feat in list(features.items()):
                if not attrib or 'before_files' in getattr(feat, 'feature_attribs', []):
                    with timing(name):
                        try:
                            feat(self)
                        except PermissionError as e:
                            error('Feature "{}" tried to change stored file "{}"'
                                  .format(name, e.filename))
                    del features[name]

    def prepare(self):
        """Prepare a task.

        Pull in files and templates, link in files from children, execute
        features and save the command. Check that all attributes have been
        consumed.
        """
        features = dict((feat, _features[feat])
                        if isinstance(feat, str)
                        else (feat.__name__, feat)
                        for feat in listify(self.consume('features')))
        self.process_features(features, 'before_files')
        with timing('files'):
            for filename in listify(self.consume('files')):
                if isinstance(filename, tuple):
                    self.store_link_file(filename[0], filename[1])
                else:
                    if '*' in filename or '?' in filename:
                        for member in glob(filename):
                            self.store_link_file(member)
                    else:
                        self.store_link_file(filename)
        with timing('hooks'):
            hooks = {filename: process_hook(filename)
                     for filename in listify(self.consume('hooks'))}
        with timing('templates'):
            templates = {}
            for filename in listify(self.consume('templates')):
                if isinstance(filename, tuple):
                    templates[filename[1]] = Template(filename[0])
                else:
                    templates[filename] = Template(filename)
        with cd(self.path):
            self.process_features(features, 'before_templates')
            with timing('templates'):
                for target, template in templates.items():
                    processed, used = template.substitute(self.attrs)
                    self.store_link_text(processed, target, template.path.name)
                    for attr in used:
                        self.consume(attr)
            with timing('linking'):
                for linkname, link in self.links.items():
                    for symlink in link.links:
                        if isinstance(symlink, tuple):
                            target, symlink = symlink
                        else:
                            target = symlink
                        relink('{}/{}'.format(linkname, target), symlink)
            self.process_features(features)
            commands = []
            env = self.consume('_env') or {}
            for hook_path, (hook_src, hook_cmd, hook_env) in hooks.items():
                commands.append(hook_cmd)
                env.update(hook_env)
                self.store_link_text(hook_src, hook_path, label=True)
            command = self.consume('command')
            if command:
                commands.append(command)
            if commands:
                with open('command', 'w') as f:
                    f.write('\n'.join(commands))
            if env:
                with open('.caf/env', 'w') as f:
                    for var, val in env.items():
                        f.write('{}={}'.format(var, val))
            if self.attrs:
                error('Task {} has non-consumed attributs: {}'
                      .format(self, list(self.attrs)))

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
                            error('Cannot link to absolute paths in tasks')
                        if str(filepath) in self.files:
                            hashes[str(filepath)] = get_file_hash(Path(target))
                        else:
                            hashes[str(filepath)] = target
                    else:
                        make_nonwritable(filepath)
                        hashes[str(filepath)] = get_file_hash(filepath)
            for linkname in self.links:
                hashes[linkname] = get_file_hash(Path(linkname)/'.caf/lock')
        return hashes

    def build(self, path):
        """Prepare, lock and store the task.

        Check if not already locked. Touch (link in children, save chilren to
        .caf/children). Check if needed children are already sealed. Check if
        children are already locked. Prepare task. Get hashes.  Lock task with
        hashes. Check if a task has been already stored previously and if not,
        store it and relink children.
        """
        with timing('task init'):
            if not path.is_dir():
                mkdir(path)
            self.path = Path(path).resolve()
            if self.is_locked():
                warn('{} already locked'.format(self))
                return
            if not self.is_touched():
                self.touch()
            for linkname, link in self.links.items():
                if link.needed and not link.task.is_sealed():
                    warn('{}: dependency "{}" not sealed'.format(self, linkname))
                    return
            if not all(child.is_locked() for child in self.children):
                return
        with timing('prepare'):
            self.prepare()
        with timing('hash'):
            hashes = self.get_hashes()
        with timing('lock'):
            self.lock(hashes)
        if 'command' not in hashes:
            with (self.path/'.caf/seal').open('w') as f:
                print('build', file=f)
        myhash = get_file_hash(self.path/'.caf/lock')
        with timing('storing'):
            cellarpath = self.ctx.cellar/str_to_path(myhash)
            if cellarpath.is_dir():
                shutil.rmtree(str(self.path))
            else:
                info('Stored new task {}'.format(self))
                mkdir(cellarpath.parent, parents=True, exist_ok=True)
                self.path.rename(cellarpath)
            self.path.symlink_to(cellarpath)
            self.path = cellarpath
        with timing('linking deps'):
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
        return NotImplemented

    def __rmul__(self, y):
        if not isinstance(y, Task):
            return NotImplemented
        return super().__radd__(y)

    def __rmatmul__(self, y):
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
        try:
            if linkname in self.targets[target]:
                error('Link "{}" already in target "{}"'.format(linkname, target))
        except TypeError:
            error('Target must be a string, not {}'.format(type(target).__name__))
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
        in_cycle = [task for task in self.tasks
                    if task._parent_counter != len(task.parents)]
        if in_cycle:
            error('There are cycles in the dependency tree')
        self.tasks = queue

    def build(self, batch):
        try:
            batch = batch.resolve()
        except FileNotFoundError:
            error('Batch does not exist, maybe `caf build new` first?')
        with timing('task sorting'):
            self.sort_tasks()
        ntskdigit = ceil(log10(len(self.tasks)+1))
        with ProgressBar(maxval=len(self.tasks), redirect_stdout=True) as progress:
            for i, task in enumerate(self.tasks):
                task.build(batch/'{:0{n}d}'.format(i, n=ntskdigit))
                progress.update(i)
        for report in _reports:
            report()

    def make_targets(self, out):
        for target, tasks in self.targets.items():
            if len(tasks) == 1 and None in tasks:
                relink(tasks[None].path, out/target)
            else:
                if not (out/target).is_dir():
                    mkdir(out/target)
                for name, task in tasks.items():
                    relink(task.path, out/target/name)

    def load_tool(self, name):
        __import__('caflib.Tools.' + name)
