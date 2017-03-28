from pathlib import Path
import re
from caflib.Logging import error, info
from hashlib import sha1


class Template:
    _cache = {}

    def __init__(self, source):
        try:
            text = source.getvalue()
        except AttributeError:
            self.key = path = Path(source)
            self.name = path.name
            if self.key not in Template._cache:
                try:
                    with path.open() as f:
                        Template._cache[self.key] = f.read()
                except FileNotFoundError:
                    error(f'Template "{path}" does not exist')
        else:
            self.key = sha1(text.encode()).hexdigest()
            self.name = self.key[-7:]
            if self.key not in Template._cache:
                Template._cache[self.key] = text

    def render(self, mapping):
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
                except Exception:
                    error(
                        'There was an error when processing default '
                        f'of key "{token}" in template "{self.name}"'
                    )
            else:
                error(
                    f'"{token}" not defined when processing template {self.name}'
                )
            used.add(token)
            try:
                return format(value, fmt) if fmt else str(value)
            except ValueError:
                error(
                    f'Unknown format "{fmt}" when processing key "{token}" '
                    f'in template "{self.name}"'
                )

        replaced = re.sub(
            r'\{\{\s*([^\s}]([^}]*[^\s}])?)\s*\}\}',
            replacer,
            Template._cache[self.key]
        )
        return replaced, used
