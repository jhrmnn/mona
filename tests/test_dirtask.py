from pathlib import Path

import pytest  # type: ignore

from caf2 import Rule, Session
from caf2.rules import dir_task
from caf2.errors import InvalidFileTarget


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
        sess.run_task(calcs())
        assert int(calcs()[0][1].default_result()) == 0
        assert sess.eval(analysis(calcs())) == 20


def test_invalid_file():
    with pytest.raises(InvalidFileTarget):
        with Session() as sess:
            sess.eval(dir_task('', {}))
