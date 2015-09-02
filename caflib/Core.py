from pathlib import Path
import imp
import os
from collections import namedtuple
import json
from contextlib import contextmanager
import subprocess
import hashlib
import tempfile
import shutil
import yaml
import re

NULL_SHA = 40*'0'


class File(object):
    _cache = {}

    def __init__(self, path):
        self.path = Path(path).resolve()
        if self.path not in File._cache:
            File._cache[self.path] = self.path.open().read()

    def format(self, **kwargs):
        return File._cache[self.path].format(**kwargs)


Result = namedtuple('Result', ['param', 'data'])
Task = namedtuple('Task', ['param', 'calc'])


def slugify(s):
    return re.sub(r'[^0-9a-zA-Z.-]', '-', s)


def get_sha_dir(top='.'):
    top = Path(top)
    h = hashlib.new('sha1')
    for path in top.glob('**/*'):
        with path.open() as f:
            h.update(f.read())
    return h.hexdigest()


def sha_to_path(sha, level=2, chunk=2):
    levels = []
    for l in range(level):
        levels.append(sha[l*chunk:(l+1)*chunk])
    levels.append(sha[level*chunk:])
    path = Path(levels[0])
    for l in levels[1:]:
        path = path/l
    return path


@contextmanager
def mktmpdir():
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    if Path(tmpdir).is_dir():
        shutil.rmtree(tmpdir)


@contextmanager
def cd(path):
    path = str(path)
    cwd = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(cwd)


def find_program(cmd):
    return Path(subprocess.check_output(['which', cmd]).strip()).resolve()


class Context(object):
    def __init__(self):
        if Path('HEAD').is_file():
            self.sha_repo = open('HEAD').read().strip()
            self.clean = self.sha_repo != NULL_SHA
        else:
            diff = subprocess.check_output('git status --porcelain'.split())
            if diff:
                self.clean = False
                self.sha_repo = NULL_SHA
            else:
                self.clean = True
                self.sha_repo = subprocess.check_output('git rev-parse HEAD'.split()).strip()
        self.tasks = []
        out = Path('build')
        self.rundir = out/(self.sha_repo[:7] + '_runs')
        self.datafile = out/(self.sha_repo[:7] + '_data.p')
        self.resultdir = out/(self.sha_repo[:7] + '_results')
        cscript = imp.new_module('cscript')
        exec open('cscript').read() in cscript.__dict__
        self.prepare = lambda: cscript.prepare(self)
        self.extract = lambda: cscript.extract(self)
        self.process = lambda: cscript.process(self)
        self.cafdir = Path(os.environ['HOME'])/'.caf'
        with (self.cafdir/'conf.yaml').open() as f:
            conf = yaml.load(f)
        self.top = Path(getattr(cscript, 'top', conf['top'])).resolve()
        if 'scratch' in conf:
            self.scratch = Path(conf['scratch'])
        else:
            self.scratch = Path('SCRATCH')
            if not self.scratch.is_dir():
                self.scratch.mkdir()

    def add_task(self, calc, **param):
        self.tasks.append(Task(param, calc))


class ArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return obj.tolist()
        except AttributeError:
            return super().default(obj)
