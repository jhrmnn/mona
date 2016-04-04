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
            if self.key not in Template._cache:
                Template._cache[self.key] = text
                info('Loading anonymous template')

    def substitute(self, mapping):
        used = set()

        def replacer(m):
            token = m.group(1)
            if ':' in token:
                token, fmt = token.split(':', 1)
            else:
                fmt = None
            if '=' in token:
                token, default = token.split('=', 1)
            else:
                default = None
            try_parse = re.match(r'(\w+)\[([\d-]+)\]', token)
            if try_parse:
                token, idx = try_parse
                idx = int(idx)
            else:
                idx = None
            if token in mapping:
                value = mapping[token]
                if idx:
                    value = value[idx]
            elif default:
                try:
                    value = eval(default)
                except:
                    error('There was an error when processing default of key "{}" '
                          'in template "{}"'.format(token, self.name))
            else:
                error('"{}" not defined when processing template {}'
                      .format(token, self.name))
            used.add(token)
            try:
                return format(value, fmt) if fmt else str(value)
            except ValueError:
                error('Unknown format "{}" when processing key "{}" in template "{}"'
                      .format(fmt, token, self.name))

        replaced = re.sub(r'\{\{\s*([^\s}]([^}]*[^\s}])?)\s*\}\}',
                          replacer,
                          Template._cache[self.key])
        return replaced, used
