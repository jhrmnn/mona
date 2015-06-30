#!/usr/bin/env python
from pathlib import Path
from slugify import slugify
from builtins import str  # python23
import json


def dispatch(root, tasks, preparer):
    root = Path(root)
    params = [[(k, v[1]) for k, v in t] for t in tasks]
    paths = [root/(('_'.join(slugify(str(v)) for k, v in p) or '_') + '.start')
             for p in params]
    tasks = [{k: v[0] for k, v in t} for t in tasks]
    for path, task, param in zip(paths, tasks, params):
        preparer(path, task)
        with open(str(path/'info.json'), 'w') as f:
            json.dump(dict(param), f)


def extract(path, extractor):
    path = Path(path)
    results = []
    for rundir in path.glob('*.done'):
        with open(str(rundir/'info.json')) as f:
            info = json.load(f)
        data = extractor(rundir)
        results.append((info, data))
    return results
