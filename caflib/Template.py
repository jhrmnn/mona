from pathlib import Path
import re
from caflib.Logging import error


class Template:
    _cache = {}

    def __init__(self, path):
        self.path = Path(path)
        if self.path not in Template._cache:
            try:
                Template._cache[self.path] = self.path.open().read()
            except FileNotFoundError:
                error('Template {} does not exist'.format(path))

    def substitute(self, mapping):
        used = set()

        def replacer(m):
            key = m.group(1)
            if key not in mapping:
                raise RuntimeError('{} not defined'.format(key))
            else:
                used.add(key)
                return str(mapping[key])

        with open(self.path.name, 'w') as f:
            f.write(re.sub(r'\{\{\s*(\w+)\s*\}\}',
                           replacer,
                           Template._cache[self.path]))
        return used
