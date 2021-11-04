import subprocess

from mona import Rule, Session, run_process, run_shell
from mona.dirtask import dir_task
from mona.files import File
from mona.plugins import Parallel
from tests.test_dirtask import analysis


@Rule
def shell():
    out = run_shell('expr `cat` "*" 2', input=b'2')
    return int(out)


@Rule
def process():
    out = run_process('bash', '-c', 'expr `cat` "*" 2', input=b'2')
    return int(out)


@Rule
def calcs(n):
    return [
        [
            dist,
            dir_task(
                File.from_str(
                    'script', f'#!/bin/bash\nexpr $(cat data) "*" 2; sleep {n}'
                ),
                [File.from_str('data', str(dist))],
                label=f'/calcs/dist={dist}',
            )['STDOUT'],
        ]
        for dist in range(5)
    ]


@Rule
def error():
    return int('x')


def test_shell():
    with Session([Parallel()]) as sess:
        assert sess.eval(shell()) == 4


def test_process():
    with Session([Parallel()]) as sess:
        assert sess.eval(process()) == 4


def test_calc():
    with Session([Parallel()]) as sess:
        assert sess.eval(analysis(calcs(0))) == 20


def test_exception():
    def handler(task, exc):
        if isinstance(exc, subprocess.CalledProcessError):
            return True

    with Session([Parallel()]) as sess:
        sess.eval(analysis(calcs('x')), exception_handler=handler)
