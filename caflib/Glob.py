# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import re


regexes = {}


def match_glob(path, pattern):
    if pattern in regexes:
        regex = regexes[pattern]
    else:
        regex = re.compile(
            pattern
            .replace('(', '\(')
            .replace(')', '\)')
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
