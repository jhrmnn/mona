# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import numpy as np  # type: ignore

from typing import Any


def p2f(value: Any, nospace: bool = False) -> str:
    if isinstance(value, bool):
        return f'.{str(value).lower()}.'
    elif isinstance(value, (np.ndarray, tuple)):
        return (' ' if not nospace else ':').join(p2f(x) for x in value)
    elif isinstance(value, dict):
        return ' '.join(
            f'{p2f(k)}={p2f(v, nospace=True)}' if v is not None else f'{p2f(k)}'
            for k, v in sorted(value.items())
        )
    else:
        return str(value)
