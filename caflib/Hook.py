from pathlib import Path
from caflib.Logging import error, info, warn
import inspect
from itertools import dropwhile
from hashlib import md5


caflib_path = Path(__file__).resolve().parents[1]

_cache = {}


def process_hook(path):
    path = Path(path)
    if path in _cache:
        return _cache[path]
    env = {}
    if path.suffix == '.py':
        if not path.is_file():
            error('Hook "{}" does not exist'.format(path))
        try:
            module = __import__(path.stem)
            info('Loading hook "{}"'.format(path))
        except:
            import traceback
            traceback.print_exc()
            error('There was an error while reading hook "{}"'.format(path))
        imports = [inspect.getmodule(obj)
                   for _, obj in inspect.getmembers(module)]
        imports = [Path(i.__file__) for i in imports if i]
        imports = [i for i in imports if 'caflib' in i.parts]
        files = []
        for i in imports:
            if i.name != '__init__.py':
                files.append(i)
            else:
                warn('Hook "{}" is loading whole caflib'.format(path))
                files.extend(i.parent.glob('**/*.py'))
        cmd = 'python3 {}'.format(path)
        with path.open() as f:
            src = '\n' + f.read()
        if files:
            env['PYTHONPATH'] = '$PYTHONPATH:{}'.format(caflib_path)
            for file in files:
                with file.open() as f:
                    h = md5(f.read().encode()).hexdigest()
                src = '# md5 {}: {}\n{}' \
                    .format('/'.join(dropwhile(lambda x: x != 'caflib', file.parts)),
                            h, src)
        _cache[path] = (src, cmd, env)
    else:
        error('Unknown hook file type: {}'.format(path))
    return _cache[path]
