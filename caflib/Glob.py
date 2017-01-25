import re


regexes = {}


def match_glob(path, pattern):
    if pattern in regexes:
        regex = regexes[pattern]
    else:
        regex = re.compile(
            pattern
            .replace('**', r'\\')
            .replace('*', '[^/]*')
            .replace(r'\\', '.*')
            .replace('<>', '([^/]*)') +
            '$'
        )
        regexes[pattern] = regex
    m = regex.match(path)
    if not m:
        return
    for group in m.groups():
        pattern = pattern.replace('<>', group, 1)
    return pattern
