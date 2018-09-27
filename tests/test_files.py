import shutil
from pathlib import Path

import pytest  # type: ignore

from caf2 import Session, FileManager
from caf2.hashing import HashedBytes
from caf2.files import StoredHashedBytes
from caf2.errors import UnknownFile

from tests.test_dirtask import calcs


def test_hashing(tmpdir):
    def run(fmngr=None):
        with Session() as sess:
            if fmngr:
                fmngr(sess)
            sess.run_task(calcs())
            fut = calcs().future_result()
            sess.eval(calcs())
            task = fut.resolve()[0][1].task
            return task, task.resolve().resolve()['STDOUT']
    fmngr = FileManager(tmpdir)
    with_fmngr = run(fmngr)
    without_fmngr = run()
    assert len(fmngr._cache) == 12
    assert len(list(Path(tmpdir).glob('*/*'))) == 12
    assert isinstance(with_fmngr[1], StoredHashedBytes)
    assert isinstance(without_fmngr[1], HashedBytes)
    assert with_fmngr[0].hashid == without_fmngr[0].hashid
    assert with_fmngr[1].hashid == without_fmngr[1].hashid


def test_missing_file(tmpdir):
    fmngr = FileManager(tmpdir)
    with pytest.raises(UnknownFile):
        with Session() as sess:
            fmngr(sess)
            sess.run_task(calcs())
            shutil.rmtree(tmpdir)
            fmngr._cache.clear()
            sess.eval(calcs())
