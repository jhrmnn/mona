import subprocess
from pathlib import Path

from caf import Session
from caf.plugins import TmpdirManager
from caf.rules import dir_task

from tests.test_dirtask import analysis, calcs


def test_basic(tmpdir):
    with Session([TmpdirManager(tmpdir)]) as sess:
        assert sess.eval(analysis(calcs())) == 20


def test_error(tmpdir):
    with Session([TmpdirManager(tmpdir)]) as sess:
        try:
            sess.eval(dir_task(b'#!/bin/bash\nxxx', {}))
        except subprocess.CalledProcessError:
            pass
    assert len(list(Path(tmpdir).glob('*/*'))) == 3
