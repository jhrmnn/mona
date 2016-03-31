from pathlib import Path
import re
from caflib.Logging import error, info


class Template:
    _cache = {}

    def __init__(self, path):
        self.path = Path(path)
        if self.path not in Template._cache:
            try:
                Template._cache[self.path] = self.path.open().read()
                info('Loading template "{}"'.format(self.path))
            except FileNotFoundError:
                error('Template "{}" does not exist'.format(path))

    def substitute(self, mapping):
        used = set()

        def replacer(m):
            token = m.group(1)
            if ':' in token:
                key, fmt = token.split(':', 1)
            else:
                key, fmt = token, None
            if key not in mapping:
                error('"{}" not defined'.format(key))
            else:
                used.add(key)
                try:
                    return format(mapping[key], fmt) if fmt else str(mapping[key])
                except ValueError:
                    error('Unknown format "{}" when processing key "{}" in template "{}"'
                          .format(fmt, key, self.path))

        replaced = re.sub(r'\{\{\s+([\w:]+)\s+\}\}',
                          replacer,
                          Template._cache[self.path])
        return replaced, used
