import numpy as np


def p2f(value, nospace=False):
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
