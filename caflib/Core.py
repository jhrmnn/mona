from pathlib import Path
import os
# import re
# import hashlib
import shutil
# from string import Template
from contextlib import contextmanager
import subprocess
import json

# NULL_SHA = 40*'0'


# class File:
#     _cache = {}
#
#     def __init__(self, path):
#         self.path = Path(path)
#         self.full_path = self.path.resolve()
#         if self.full_path not in File._cache:
#             File._cache[self.full_path] = Template(self.path.open().read())
#
#     def substitute(self, mapping):
#         with self.path.open('w') as f:
#             f.write(File._cache[self.full_path].substitute(mapping))


# def slugify(s):
#     return re.sub(r'[^0-9a-zA-Z.-]', '-', s)


# def get_sha_dir(top='.'):
#     top = Path(top)
#     h = hashlib.new('sha1')
#     for path in sorted(top.glob('**/*')):
#         h.update(str(path).encode())
#         with path.open('rb') as f:
#             h.update(f.read())
#     return h.hexdigest()


# def sha_to_path(sha, level=2, chunk=2):
#     levels = []
#     for l in range(level):
#         levels.append(sha[l*chunk:(l+1)*chunk])
#     levels.append(sha[level*chunk:])
#     path = Path(levels[0])
#     for l in levels[1:]:
#         path = path/l
#     return path


# @contextmanager
# def mktmpdir(prefix):
#     tmpdir = Path(prefix)/sha_to_path(NULL_SHA)
#     if tmpdir.is_dir():
#         shutil.rmtree(str(tmpdir))
#     tmpdir.mkdir(parents=True)
#     yield str(tmpdir)
#     if Path(tmpdir).is_dir():
#         shutil.rmtree(str(tmpdir))


@contextmanager
def cd(path):
    path = str(path)
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


def mkdir(path):
    subprocess.check_call(['mkdir', '-p', str(path)])


def listify(obj):
    if not obj:
        return []
    if isinstance(obj, str):
        return [obj]
    try:
        return [x for x in obj]
    except TypeError:
        return [obj]


# def prepare(ctx):
#     ctx.prepare()
#     task_db = []
#     for param, calc in ctx.tasks:
#         with mktmpdir(ctx.cache) as tmpdir:
#             with cd(tmpdir):
#                 with open('command', 'w') as f:
#                     f.write(calc.command)
#                 calc.prepare()
#                 sha_dir = get_sha_dir()
#             path = ctx.cache/'objects'/sha_to_path(sha_dir)
#             if not path.is_dir():
#                 if not path.parent.is_dir():
#                     path.parent.mkdir(parents=True)
#                 shutil.move(tmpdir, str(path))
#         stem = '_'.join('{}={}'.format(key, slugify(str(value)))
#                         for key, value in param.items()) or '_'
#         path_run = ctx.rundir/stem
#         path_run.symlink_to(path if path.is_absolute() else Path('../..')/path)
#         task_db.append((param, str(path_run)))
#     with (ctx.rundir/'tasks.json').open('w') as f:
#         json.dump(task_db, f, indent=4)


class Task:
    def __init__(self, **attrs):
        self.attrs = attrs
        self.children = []
        self.parents = []

    def __radd__(self, obj):
        try:
            for link in obj:
                link + self
        except TypeError:
            assert isinstance(obj, Link)
            obj.child.parents.append(obj)
            self.children.append(obj)
        return self

    def consume(self, attr):
        return self.attrs.pop(attr, None)

    def touch(self):
        mkdir(self.path/'.caf')

    def is_touched(self):
        return (self.path/'.caf').is_dir()

    def lock(self):
        (self.path/'.caf/lock').touch()

    def is_locked(self):
        return (self.path/'.caf/lock').is_file()

    def seal(self):
        (self.path/'.caf/seal').touch()

    def is_sealed(self):
        return (self.path/'.caf/seal').is_file()

    def set_path(self, path):
        self.path = path
        for lnk in self.children:
            lnk.child.set_path(path/lnk.name)

    def build(self):
        if not self.is_touched():
            self.touch()
            with (self.path/'.caf/children').open('w') as f:
                json.dump([lnk.name for lnk in self.children], f)
        if self.is_locked():
            print('{} already locked'.format(self))
            return
        for lnk in self.children:
            if lnk.needed and not lnk.child.is_sealed():
                print('{} not sealed'.format(lnk.child))
                return
        mkdir(self.path)
        for filename in listify(self.consume('files')):
            shutil.copy(filename, str(self.path))
        with cd(self.path):
            for lnk in self.children:
                for here, there in lnk.links.items():
                    os.system('ln -s {}/{} {}'.format(lnk.name, there, here))
            for feat in listify(self.consume('features')):
                try:
                    feat(self)
                except Exception as e:
                    print(e)
                    return
            with open('command', 'w') as f:
                f.write(self.consume('command'))
        if self.attrs:
            print('task has non-consumed attributs {}'.format(self.attrs.keys()))
        self.lock()


class Link:
    def __init__(self, name, links=None, needed=False):
        if isinstance(name, str):
            self.name = name
        elif isinstance(name, tuple):
            self.name = '_'.join(str(x) for x in name)
        elif isinstance(name, dict):
            self.name = '_'.join('{}={}'.format(k, v) for k, v in name.items())
        self.links = links
        self.needed = needed

    def __radd__(self, obj):
        assert isinstance(obj, Task)
        self.child = obj
        return self


class View:
    def __init__(self, name):
        self.name = name
        self.children = []

    def __radd__(self, obj):
        try:
            for link in obj:
                link + self
        except TypeError:
            assert isinstance(obj, Link)
            self.children.append(obj)
        return self

    def set_path(self, path):
        for lnk in self.children:
            lnk.child.set_path(path/lnk.name)


class Context:
    def __init__(self):
        self.tasks = []
        self.views = []
        self.link = Link

    def add_task(self, **kwargs):
        task = Task(**kwargs)
        self.tasks.append(task)
        return task

    __call__ = add_task

    def view(self, *args, **kwargs):
        view = View(*args, **kwargs)
        self.views.append(view)
        return view

    def set_paths(self):
        for view in self.views:
            path = Path('build/latest')/view.name
            view.set_path(path)

    def build(self):
        self.set_paths()
        for task in self.tasks:
            task.build()
