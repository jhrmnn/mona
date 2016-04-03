from pathlib import Path
import re
from caflib.Logging import error, info
from hashlib import sha1


class Template:
    _cache = {}

    def __init__(self, path_or_stringio):
        try:
            text = path_or_stringio.getvalue()
        except AttributeError:
            self.key = path = Path(path_or_stringio)
            self.name = path.name
            if self.key not in Template._cache:
                try:
                    Template._cache[self.key] = path.open().read()
                    info('Loading template "{}"'.format(path))
                except FileNotFoundError:
                    error('Template "{}" does not exist'.format(path))
        else:
            self.key = sha1(text.encode()).hexdigest()
            self.name = self.key[-7:]
            Template._cache[self.key] = text
            info('Loading anonymous template')

    def substitute(self, mapping):
        used = set()

        def replacer(m):
            token = m.group(1)
            if ':' in token:
                key, fmt = token.split(':', 1)
            else:
                key, fmt = token, None
            if key not in mapping:
                error('"{}" not defined when processing template {}'.format(key, self.name))
            else:
                used.add(key)
                try:
                    return format(mapping[key], fmt) if fmt else str(mapping[key])
                except ValueError:
                    error('Unknown format "{}" when processing key "{}" in template "{}"'
                          .format(fmt, key, self.name))

        replaced = re.sub(r'\{\{\s+([\w:]+)\s+\}\}',
                          replacer,
                          Template._cache[self.key])
        return replaced, used
