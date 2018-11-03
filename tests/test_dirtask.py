from pathlib import Path

import pytest  # type: ignore

from mona import Rule, Session
from mona.files import Source
from mona.rules import dir_task
from mona.errors import InvalidInput


@Rule
async def calcs():
    return [
        [
            dist,
            dir_task(
                Source('script', '#!/bin/bash\nexpr $(cat input) "*" 2; true'),
                [Source('data', str(dist)), [Path('input'), 'data']],
                label=f'/calcs/dist={dist}',
            )['STDOUT'],
        ]
        for dist in range(5)
    ]


@Rule
async def analysis(results):
    return sum(int(stdout.read_text()) for _, stdout in results)


@Rule
async def python():
    return dir_task(
        Source(
            'script', '#!/usr/bin/env python\nimport coverage\nprint(coverage.__name__)'
        ),
        [],
    )['STDOUT']


def test_calc():
    with Session() as sess:
        sess.run_task(calcs())
        assert sess.eval(analysis(calcs())) == 20


def test_invalid_file():
    with pytest.raises(InvalidInput):
        with Session() as sess:
            sess.eval(dir_task('', {}))


def test_python():
    with Session() as sess:
        assert sess.eval(python()).read_text().rstrip() == 'coverage'
