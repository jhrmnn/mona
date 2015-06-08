from pathlib import Path
from slugify import slugify
import yaml


def dispatch(path, tasks, preparer):
    path = Path(path)
    params = [[(k, v[1]) for k, v in t] for t in tasks]
    paths = [path/('_'.join(slugify(unicode(v)) for k, v in p)
                   + '.start')
             for p in params]
    tasks = [{k: v[0] for k, v in t} for t in tasks]
    for path, task, param in zip(paths, tasks, params):
        preparer(path, task)
        with (path/'info.yaml').open('w') as f:
            f.write(yaml.dump(dict(param),
                              encoding=None,
                              default_flow_style=False))


def extract(path, extractor):
    path = Path(path)
    results = []
    for rundir in path.glob('*.done'):
        with (rundir/'info.yaml').open() as f:
            info = yaml.load(f)
        data = extractor(rundir)
        results.append((info, data))
    return results
