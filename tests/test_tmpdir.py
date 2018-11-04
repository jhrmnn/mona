import subprocess
from pathlib import Path

from mona import Session
from mona.files import HashedFile
from mona.plugins import TmpdirManager
from mona.rules import dir_task

from tests.test_dirtask import analysis, calcs


def test_basic(tmpdir):
    with Session([TmpdirManager(tmpdir)]) as sess:
        assert sess.eval(analysis(calcs())) == 20


def test_error(tmpdir):
    with Session([TmpdirManager(tmpdir)]) as sess:
        try:
            sess.eval(dir_task(HashedFile('script', '#!/bin/bash\nxxx'), []))
        except subprocess.CalledProcessError:
            pass
    assert len(list(Path(tmpdir).glob('*/*'))) == 3
