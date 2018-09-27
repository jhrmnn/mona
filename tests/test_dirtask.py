from pathlib import Path

import pytest  # type: ignore

from caf2 import Rule, Session
from caf2.rules import dir_task


@Rule
def calcs():
    return [[
        dist,
        dir_task(
            '#!/bin/bash\nexpr $(cat input) "*" 2; true'.encode(),
            {'data': str(dist).encode(), 'input': Path('data')},
            label=f'/calcs/dist={dist}'
        ).get('STDOUT', b'0')
    ] for dist in range(5)]


@Rule
def analysis(results):
    return sum(int(res) for _, res in results)


def test_calc():
    with Session() as sess:
        assert sess.eval(analysis(calcs())) == 20
