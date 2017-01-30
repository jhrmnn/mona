import re


regexes = {}


def match_glob(path, pattern):
    if pattern in regexes:
        regex = regexes[pattern]
    else:
        regex = re.compile(
            pattern
            .replace('?', '[^/]')
            .replace('<>', '([^/]*)')
            .replace('<', '(')
            .replace('>', ')')
            .replace('{', '(?:')
            .replace('}', ')')
            .replace(',', '|')
            .replace('**', r'\\')
            .replace('*', '[^/]*')
            .replace(r'\\', '.*') + '$'
        )
        regexes[pattern] = regex
    m = regex.match(path)
    if not m:
        return
    for group in m.groups():
        pattern = re.sub(r'<.*?>', group, pattern, 1)
    return pattern
