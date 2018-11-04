import shutil
from pathlib import Path

import pytest  # type: ignore

from mona import Rule, Session
from mona.rules import dir_task
from mona.plugins import FileManager
from mona.files import HashedFile, Source
from mona.errors import FilesError

from tests.test_dirtask import calcs


@pytest.fixture(scope='module')
def datafile():
    path = Path('data')
    path.write_text(str(2))
    try:
        yield path
    finally:
        path.unlink()


@Rule
async def calcs2():
    return [
        [
            dist,
            dir_task(
                HashedFile('script', '#!/bin/bash\nexpr $(cat input) "*" 2; true'),
                [HashedFile('data', str(dist)), [Path('input'), 'data']],
                label=f'/calcs/dist={dist}',
            ).get('STDOUT', b'0'),
        ]
        for dist in range(5)
    ]


def test_hashing(tmpdir):
    def run(calcs, fmngr=None):
        with Session([fmngr] if fmngr else []) as sess:
            sess.run_task(calcs())
            fut = calcs().future_result()
            sess.eval(calcs())
            task = fut.resolve()[0][1].task
            output = task.resolve().resolve()['STDOUT']
            return task, output

    fmngr = FileManager(tmpdir)
    without_fmngr = run(calcs)
    with_fmngr = run(calcs, fmngr)
    with Session([fmngr]) as sess:
        task = dir_task(
            HashedFile('script', '#!/bin/bash\nexpr $(cat input) "*" 2; true'),
            [HashedFile('data', str(0)), [Path('input'), 'data']],
        )
        sess.run_task(task)
        alt_input = task, task.resolve().resolve()['STDOUT']
    alt_input = run(calcs2, fmngr)
    assert len(fmngr._cache) == 12
    assert len(list(Path(tmpdir).glob('*/*'))) == 12
    assert isinstance(without_fmngr[1], HashedFile)
    assert isinstance(with_fmngr[1], HashedFile)
    assert isinstance(alt_input[1], HashedFile)
    assert without_fmngr[0].hashid == with_fmngr[0].hashid
    assert without_fmngr[0].hashid == alt_input[0].hashid
    assert without_fmngr[1].hashid == with_fmngr[1].hashid
    assert without_fmngr[1].hashid == alt_input[1].hashid


def test_postponed(tmpdir):
    fmngr = FileManager(tmpdir, eager=False)
    with Session([fmngr]) as sess:
        sess.eval(calcs2(), task_filter=lambda t: t.label[0] != '/')
        assert len(list(Path(tmpdir).glob('**'))) == 1
    fmngr.store_cache()
    assert len(list(Path(tmpdir).glob('**'))) == 7


def test_missing_file(tmpdir):
    fmngr = FileManager(tmpdir)
    with pytest.raises(FilesError):
        with Session([fmngr]) as sess:
            sess.run_task(calcs())
            shutil.rmtree(tmpdir)
            fmngr._cache.clear()
            sess.eval(calcs())


def test_access(tmpdir):
    fmngr = FileManager(tmpdir)
    with Session([fmngr]) as sess:
        sess.run_task(calcs())
        fut = calcs().future_result()
        sess.eval(calcs())
        task = fut.resolve()[0][1].task
        output = task.resolve().resolve()['STDOUT']
        assert int(output.value.read_text()) == 0
        shutil.rmtree(tmpdir)
        fmngr._cache.clear()
        with pytest.raises(FilesError):
            output.value.read_text()


def test_alt_input(datafile, tmpdir):
    def create_task():
        return dir_task(
            HashedFile('script', '#!/bin/bash\nexpr $(cat input) "*" 2; true'),
            [Source('data'), [Path('input'), 'data']],
        )

    with Session([FileManager(tmpdir)]) as sess:
        sess.run_task(create_task())
        int(create_task().value['STDOUT'].read_text()) == 4


def test_alt_input2(datafile, tmpdir):
    with Session([FileManager(tmpdir)]) as sess:
        task = dir_task(
            HashedFile('script', '#!/bin/bash\nexpr $(cat input) "*" 2; true'),
            [Source('data'), [Path('input'), 'data']],
        )
        assert int(sess.run_task(task).value['STDOUT'].read_text()) == 4
