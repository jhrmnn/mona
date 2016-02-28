from pathlib import Path
from caflib.Logging import error, info, warn
from caflib.Utils import report
import inspect
from itertools import dropwhile
from hashlib import md5
from collections import defaultdict


caflib_path = Path(__file__).resolve().parents[1]

cache = {}
filetypes = {'.py': 'python'}
_reported = []


@report
def reporter():
    for printer, msg in _reported:
        printer(msg)


def check_caflib(path, src, env):
    try:
        module = __import__(path.stem)
        info('Loading hook "{}"'.format(path))
    except:
        import traceback
        traceback.print_exc()
        error('There was an error while reading hook "{}"'.format(path))
    imports = [inspect.getmodule(obj)
               for _, obj in inspect.getmembers(module)]
    imports = set(Path(i.__file__) for i in imports if i)
    imports = [i for i in imports if 'caflib' in i.parts]
    files = []
    for i in imports:
        if i.name != '__init__.py':
            files.append(i)
        else:
            _reported.append((
                warn, 'Hook "{}" is loading whole caflib'.format(path)))
            files.extend(i.parent.glob('**/*.py'))
    files = sorted(set(files))
    if files:
        env['PYTHONPATH'].append(caflib_path)
        for file in files:
            with file.open() as f:
                h = md5(f.read().encode()).hexdigest()
            src = '{}\n# md5 {}: {}' \
                .format(src,
                        '/'.join(dropwhile(lambda x: x != 'caflib', file.parts)),
                        h)
    return src


dependencies = {'python': [check_caflib]}


def process_hook(path):
    path = Path(path)
    if not path.is_file():
        error('Hook "{}" does not exist'.format(path))
    if path in cache:
        return cache[path]
    filetype = filetypes.get(path.suffix)
    if not filetype:
        error('Unknown hook file type: {}'.format(path))
    with path.open() as f:
        src = f.read()
    env = defaultdict(list)
    for dependency in dependencies[filetype]:
        src = dependency(path, src, env)
    if filetype == 'python':
        if 'PYTHONPATH' in env:
            env['PYTHONPATH'].append('$PYTHONPATH')
        cmd = 'python3 {}'.format(path)
    cache[path] = (src, cmd, env)
    return cache[path]
