from pathlib import Path
import re


class Template:
    _cache = {}

    def __init__(self, path):
        self.path = Path(path)
        if self.path not in Template._cache:
            Template._cache[self.path] = self.path.open().read()

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
