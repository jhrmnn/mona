from pathlib import Path

import pytest  # type: ignore

from caf import Rule, Session
from caf.rules import dir_task
from caf.errors import InvalidInput


@Rule
async def calcs():
    return [[
        dist,
        dir_task(
            '#!/bin/bash\nexpr $(cat input) "*" 2; true'.encode(),
            {'data': str(dist).encode(), 'input': Path('data')},
            label=f'/calcs/dist={dist}'
        ).get('STDOUT', b'0')
    ] for dist in range(5)]


@Rule
async def analysis(results):
    return sum(int(res) for _, res in results)


@Rule
async def python():
    return dir_task(
        '#!/usr/bin/env python\n'
        'import coverage\n'
        'print(coverage.__name__)'.encode(),
        {}
    )['STDOUT']


def test_calc():
    with Session() as sess:
        sess.run_task(calcs())
        assert int(calcs()[0][1].default_result()) == 0
        assert sess.eval(analysis(calcs())) == 20


def test_invalid_file():
    with pytest.raises(InvalidInput):
        with Session() as sess:
            sess.eval(dir_task('', {}))


def test_python():
    with Session() as sess:
        assert sess.eval(python()).decode().rstrip() == 'coverage'
