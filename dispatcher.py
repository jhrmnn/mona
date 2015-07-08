#!/usr/bin/env python
from pathlib import Path
from slugify import slugify
import json
from collections import namedtuple
import cPickle as pickle


Row = namedtuple('Row', ['info', 'data'])
Parameter = namedtuple('Parameter', ['key', 'str', 'value'])
Parameter.__new__.__defaults__ = (None,)


class ArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return obj.tolist()
        except AttributeError:
            return super().default(obj)


def dispatch(root, tasks, preparer):
    root = Path(root)
    tasks = [[Parameter(*p) for p in t] for t in tasks]
    for task in tasks:
        stem = '_'.join(slugify(unicode(p.str)) for p in task if p.str) or '_'
        path = root/(stem + '.start')
        path.mkdir(parents=True)
        preparer(path, {p.key: p.value for p in task if p.value})
        with open(str(path/'info.json'), 'w') as f:
            json.dump({p.key: p.str for p in task if p.str}, f)


def extract(path, extractor):
    path = Path(path)
    results = []
    for rundir in path.glob('*.done'):
        with open(str(rundir/'info.json')) as f:
            info = json.load(f)
        try:
            data = extractor(rundir/'rundir')
        except:
            print('info: Error occured in {}'.format(rundir))
            raise
        results.append(Row(info, data))
    with (path/'results.p').open('wb') as f:
        pickle.dump(results, f, -1)


def fetch():
    with open('results.p', 'rb') as f:
        return pickle.load(f)
