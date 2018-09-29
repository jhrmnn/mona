from caf2 import Session, Rule, run_shell, run_process, run_thread
from caf2.rules import dir_task
from caf2.plugins import Parallel

from tests.test_dirtask import analysis


@Rule
async def shell():
    out, _ = await run_shell('expr `cat` "*" 2', input=b'2')
    return int(out)


@Rule
async def process():
    out, _ = await run_process('bash', '-c', 'expr `cat` "*" 2', input=b'2')
    return int(out)


@Rule
async def calcs():
    return [[
        dist,
        dir_task(
            '#!/bin/bash\nexpr $(cat data) "*" 2; true'.encode(),
            {'data': str(dist).encode()},
            label=f'/calcs/dist={dist}'
        )['STDOUT']
    ] for dist in range(5)]


def test_shell():
    with Session([Parallel()]) as sess:
        sess.eval(shell()) == 4


def test_process():
    with Session([Parallel()]) as sess:
        sess.eval(process()) == 4


def test_thread():
    @Rule
    async def f():
        return await run_thread(lambda: 4)

    with Session([Parallel()]) as sess:
        sess.eval(f()) == 4


def test_calc():
    with Session([Parallel()]) as sess:
        assert sess.eval(analysis(calcs())) == 20
