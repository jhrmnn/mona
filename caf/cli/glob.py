# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import re

from typing import Dict, Pattern, Optional

_regexes: Dict[str, Pattern[str]] = {}


def match_glob(path: str, pattern: str) -> Optional[str]:
    regex = _regexes.get(pattern)
    if not regex:
        regex = re.compile(
            pattern.replace('(', r'\(')
            .replace(')', r'\)')
            .replace('?', '[^/]')
            .replace('<>', '([^/]*)')
            .replace('<', '(')
            .replace('>', ')')
            .replace('{', '(?:')
            .replace('}', ')')
            .replace(',', '|')
            .replace('**', r'\\')
            .replace('*', '[^/]*')
            .replace(r'\\', '.*')
            + '$'
        )
        _regexes[pattern] = regex
    m = regex.match(path)
    if not m:
        return None
    for group in m.groups():
        pattern = re.sub(r'<.*?>', group, pattern, 1)
    return pattern
